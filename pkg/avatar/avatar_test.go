package avatar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeImage builds a deterministic RGBA test image of the given size.
func makeImage(w, h int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 7, A: 255})
		}
	}
	return img
}

func encodePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, makeImage(w, h)); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func encodeJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, makeImage(w, h), nil); err != nil {
		t.Fatalf("encode jpeg: %v", err)
	}
	return buf.Bytes()
}

func encodeGIF(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gif.Encode(&buf, makeImage(w, h), nil); err != nil {
		t.Fatalf("encode gif: %v", err)
	}
	return buf.Bytes()
}

// pngHeaderClaiming returns the bytes of a file a PNG decoder will accept as
// a PNG whose IHDR declares the given dimensions, without encoding any of the
// pixels it claims: 20000x20000 costs a handful of bytes on the wire and
// 1.6 GB if anything honours the header. That gap is what the pixel ceiling
// exists to close.
func pngHeaderClaiming(t *testing.T, w, h int) []byte {
	t.Helper()
	buf := bytes.NewBuffer([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'})
	ihdr := make([]byte, 13) // 8-bit RGBA; compression, filter and interlace stay zero
	binary.BigEndian.PutUint32(ihdr[0:4], uint32(w))
	binary.BigEndian.PutUint32(ihdr[4:8], uint32(h))
	ihdr[8] = 8
	ihdr[9] = 6
	writePNGChunk(buf, "IHDR", ihdr)
	return buf.Bytes()
}

func writePNGChunk(buf *bytes.Buffer, kind string, data []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	buf.Write(length[:])
	buf.WriteString(kind)
	buf.Write(data)
	crc := crc32.NewIEEE()
	_, _ = crc.Write([]byte(kind))
	_, _ = crc.Write(data)
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc.Sum32())
	buf.Write(sum[:])
}

// The pixel ceiling is what stands between a small file and a large
// allocation: the byte cap bounds the wire, and the header a decoder reads
// before it allocates is the source's own claim about its dimensions.
func TestNormaliseRefusesOverlargeDeclaredDimensions(t *testing.T) {
	if _, err := normalise(pngHeaderClaiming(t, 20000, 20000)); !errors.Is(err, errTooLarge) {
		t.Fatalf("err = %v, want errTooLarge", err)
	}
	// The source dimensions the issue says the forge returns stay well inside.
	if _, err := normalise(encodePNG(t, 592, 592)); err != nil {
		t.Errorf("normalise of a 592x592 source: %v, want no error", err)
	}
}

// forgeStub is a stand-in for the forge: it answers the users API with an
// avatar_url and serves same-origin image paths, counting both kinds of
// request so tests can assert the proxy makes exactly two.
type forgeStub struct {
	srv       *httptest.Server
	apiHits   atomic.Int32
	imageHits atomic.Int32

	mu          sync.Mutex
	avatarURL   string // what the API names for the standing login
	imageBody   []byte
	imageType   string
	imageStatus int
	apiStatus   int
	redirectTo  string
	// The image hop's real headers (issue #276): a private cache hint and
	// Last-Modified, and no ETag at all. imageETag is only set by the test
	// that plants one, to prove the proxy never forwards it.
	imageETag         string
	imageLastModified string
	honorIfModSince   bool   // answer a matching conditional image request 304
	ifModSince        string // If-Modified-Since carried by the latest image request
}

func newForgeStub(t *testing.T) *forgeStub {
	t.Helper()
	f := &forgeStub{
		imageBody:   encodePNG(t, 309, 309),
		imageType:   "image/png",
		imageStatus: http.StatusOK,
		apiStatus:   http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		f.apiHits.Add(1)
		f.mu.Lock()
		avatarURL, apiStatus := f.avatarURL, f.apiStatus
		f.mu.Unlock()
		if apiStatus != http.StatusOK {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(apiStatus)
			// What the mTLS ingress actually answers a rejected request with.
			_, _ = w.Write([]byte("<html><head><title>400 No required SSL certificate was sent</title></head></html>"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"username":   strings.TrimPrefix(r.URL.Path, "/api/v1/users/"),
			"avatar_url": avatarURL,
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.imageHits.Add(1)
		f.mu.Lock()
		redirectTo, status, ctype, body := f.redirectTo, f.imageStatus, f.imageType, f.imageBody
		etag, lastModified := f.imageETag, f.imageLastModified
		f.ifModSince = r.Header.Get("If-Modified-Since")
		conditional := f.honorIfModSince && f.ifModSince != "" && f.ifModSince == lastModified
		f.mu.Unlock()
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		if lastModified != "" {
			w.Header().Set("Last-Modified", lastModified)
		}
		w.Header().Set("Cache-Control", "private, max-age=21600")
		if redirectTo != "" {
			http.Redirect(w, r, redirectTo, http.StatusFound)
			return
		}
		if conditional {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", ctype)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	f.srv = httptest.NewServer(mux)
	f.avatarURL = f.srv.URL + "/avatars/" + strings.Repeat("a", 64) + ".png"
	t.Cleanup(f.srv.Close)
	return f
}

func (f *forgeStub) totalHits() int {
	return int(f.apiHits.Load() + f.imageHits.Load())
}

func newProxy(t *testing.T, f *forgeStub) (*Proxy, string) {
	t.Helper()
	dir := t.TempDir()
	p, err := New(Config{ForgeURL: f.srv.URL, CacheDir: dir})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}
	return p, dir
}

func get(p *Proxy, login string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, (&url.URL{Path: PathPrefix + login}).String(), nil))
	return rec
}

// storeFiles lists the names of the files the proxy has stored.
func storeFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// Happy path: API, then one GET of avatar_url; the stored render is a
// normalised PNG tile served to the caller.
func TestServeFetchesAPIThenImageOnceAndStoresTile(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	rec := get(p, "s-autonomics-implementer")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := f.totalHits(); got != 2 {
		t.Errorf("forge hits = %d, want exactly 2 (API + image)", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
		t.Errorf("Content-Type = %q, want image/png", ct)
	}
	img, _, err := image.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("response body is not a decodable image: %v", err)
	}
	if b := img.Bounds(); b.Dx() != DefaultTileSize || b.Dy() != DefaultTileSize {
		t.Errorf("served tile = %dx%d, want %dx%d", b.Dx(), b.Dy(), DefaultTileSize, DefaultTileSize)
	}
	names := storeFiles(t, dir)
	if len(names) != 1 || names[0] != "s-autonomics-implementer.png" {
		t.Errorf("store = %v, want exactly [s-autonomics-implementer.png]", names)
	}
}

// A resize is performed once: the second request is served from the stored
// render without touching the forge again.
func TestServeServesStoredRenderWithoutRefetching(t *testing.T) {
	f := newForgeStub(t)
	p, _ := newProxy(t, f)

	first := get(p, "alice")
	hitsAfterFirst := f.totalHits()
	second := get(p, "alice")

	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, want 200", second.Code)
	}
	if got := f.totalHits(); got != hitsAfterFirst {
		t.Errorf("forge hits grew to %d on a warm request, want %d", got, hitsAfterFirst)
	}
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Error("warm response differs from the stored render")
	}
}

// The ingress answers 4xx with an HTML body; that is a miss and stores
// nothing — never a body.
func TestServeIngressHTMLErrorIsMissAndStoresNothing(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.apiStatus = http.StatusBadRequest
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")

	if rec.Code == http.StatusOK {
		t.Fatal("4xx-with-HTML must be a miss, got 200")
	}
	if strings.Contains(rec.Body.String(), "<html>") || strings.Contains(rec.Body.String(), "SSL certificate") {
		t.Errorf("upstream error body leaked to the caller: %q", rec.Body.String())
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after a miss, got %v", files)
	}
}

// A non-image content type on the avatar fetch is a miss, never a body.
func TestServeNonImageContentTypeIsMissAndStoresNothing(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageType = "text/html"
	f.imageBody = []byte("<html><body>gotcha</body></html>")
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")

	if rec.Code == http.StatusOK {
		t.Fatal("non-image content type must be a miss, got 200")
	}
	if strings.Contains(rec.Body.String(), "gotcha") {
		t.Errorf("non-image body leaked to the caller: %q", rec.Body.String())
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after a miss, got %v", files)
	}
}

// The byte cap bounds the wire, not the framebuffer: a decodable file may
// declare dimensions no avatar could ever have. Anything over the ceiling is a
// miss before a single pixel is allocated, and stores nothing.
func TestServeRefusesOverlargeDeclaredDimensionsBeforeDecoding(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageBody = pngHeaderClaiming(t, 20000, 20000)
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 for a source over the pixel ceiling", rec.Code)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after a pixel-ceiling miss, got %v", files)
	}
}

// The forge ignores ?size= and returns source-dimension images; the proxy
// resizes once and stores the small render.
func TestServeResizesSourceDimensionsOnce(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageBody = encodePNG(t, 592, 592)
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	stored := filepath.Join(dir, "alice.png")
	disk, err := os.ReadFile(stored)
	if err != nil {
		t.Fatalf("stored render missing: %v", err)
	}
	if len(disk) >= len(f.imageBody) {
		t.Errorf("stored render is %d bytes, source was %d: no resize happened", len(disk), len(f.imageBody))
	}
	img, err := png.Decode(bytes.NewReader(disk))
	if err != nil {
		t.Fatalf("stored render is not a PNG: %v", err)
	}
	if b := img.Bounds(); b.Dx() != DefaultTileSize || b.Dy() != DefaultTileSize {
		t.Errorf("stored render = %dx%d, want %dx%d", b.Dx(), b.Dy(), DefaultTileSize, DefaultTileSize)
	}
}

// A changed email hash — a new avatar_url for a standing login — must not
// spawn a second cache entry: the key is the login, never the URL.
func TestServeChangedEmailHashDoesNotSpawnSecondEntry(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	if rec := get(p, "alice"); rec.Code != http.StatusOK {
		t.Fatalf("first fetch: status = %d", rec.Code)
	}
	// The account's email changes: the API now names a different hash.
	f.mu.Lock()
	f.avatarURL = f.srv.URL + "/avatars/" + strings.Repeat("b", 64) + ".png"
	f.mu.Unlock()

	// While the tile is stored, the proxy must not even re-fetch.
	if rec := get(p, "alice"); rec.Code != http.StatusOK {
		t.Fatalf("warm fetch: status = %d", rec.Code)
	}
	if names := storeFiles(t, dir); len(names) != 1 {
		t.Fatalf("store = %v, want exactly one entry keyed by login", names)
	}

	// When the tile is later refetched (a revalidation, a cleared store),
	// the new hash must land on the same login-keyed entry, not beside it.
	if err := os.Remove(filepath.Join(dir, "alice.png")); err != nil {
		t.Fatalf("remove stored render: %v", err)
	}
	if rec := get(p, "alice"); rec.Code != http.StatusOK {
		t.Fatalf("refetch: status = %d", rec.Code)
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "alice.png" {
		t.Errorf("store = %v, want exactly [alice.png] after an email-hash change", names)
	}
}

// Redirects are never chased: a 3xx on the avatar fetch is a miss.
func TestServeDoesNotFollowRedirects(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.redirectTo = f.srv.URL + "/somewhere-else.png"
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")

	if rec.Code == http.StatusOK {
		t.Fatal("a redirect must be a miss, got 200")
	}
	if got := f.imageHits.Load(); got != 1 {
		t.Errorf("image hits = %d, want 1 (no redirect chasing)", got)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after a redirect miss, got %v", files)
	}
}

// avatar_url never points off the forge (site invariant); a URL on another
// origin is a miss and is never requested.
func TestServeRejectsOffForgeAvatarURL(t *testing.T) {
	f := newForgeStub(t)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("proxy fetched avatar_url off the forge: %s", r.URL.Path)
	}))
	t.Cleanup(other.Close)
	f.mu.Lock()
	f.avatarURL = other.URL + "/x.png"
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	rec := get(p, "alice")

	if rec.Code == http.StatusOK {
		t.Fatal("an off-forge avatar_url must be a miss, got 200")
	}
	if got := f.imageHits.Load(); got != 0 {
		t.Errorf("image hits = %d, want 0", got)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after an off-forge miss, got %v", files)
	}
}

// The login is the cache key and a filesystem name: anything that is not a
// bare login is refused without contacting the forge.
func TestServeRejectsUnusableLogins(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	for _, target := range []string{
		PathPrefix,                   // empty login
		PathPrefix + "..",            // traversal-shaped name
		PathPrefix + "..%2f..%2fetc", // encoded slashes
		PathPrefix + "a%20b",         // a space is not a login
		PathPrefix + "a/b",           // a slash cannot ride in a login
		PathPrefix + "%C3%BCn%C3%AFcode",
	} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("target %q: status = %d, want 400", target, rec.Code)
		}
	}
	if got := f.totalHits(); got != 0 {
		t.Errorf("forge hits = %d for invalid logins, want 0", got)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty, got %v", files)
	}
}

// A login the forge does not know is absence, not unreachability: a 200
// carrying the question-mark glyph, cacheable like any other answer, and
// nothing stored (issue #276).
func TestServeUnknownIdentityServesCachedGlyph(t *testing.T) {
	f := newForgeStub(t)
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			f.apiHits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		orig.ServeHTTP(w, r)
	})
	p, dir := newProxy(t, f)

	first := get(p, "ghost")

	if first.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with the glyph", first.Code)
	}
	img, _, err := image.Decode(bytes.NewReader(first.Body.Bytes()))
	if err != nil {
		t.Fatalf("absence was not answered with a decodable image: %v", err)
	}
	if b := img.Bounds(); b.Dx() != DefaultTileSize || b.Dy() != DefaultTileSize {
		t.Errorf("glyph = %dx%d, want %dx%d", b.Dx(), b.Dy(), DefaultTileSize, DefaultTileSize)
	}
	if got := first.Header().Get("Cache-Control"); got != cacheControlValue {
		t.Errorf("Cache-Control = %q, want the proxy's policy %q", got, cacheControlValue)
	}
	if got := first.Header().Get("ETag"); got != etagFor(first.Body.Bytes()) {
		t.Errorf("ETag = %q, want the glyph's content hash", got)
	}

	// Cached like any answer: the next viewer is served the same glyph from
	// the memory of the absence rather than a fresh pair of forge requests.
	second := get(p, "ghost")
	if second.Code != http.StatusOK || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Errorf("second request: status = %d, body equal = %v, want the same glyph",
			second.Code, bytes.Equal(second.Body.Bytes(), first.Body.Bytes()))
	}
	if got := f.apiHits.Load(); got != 1 {
		t.Errorf("API hits = %d, want 1: the remembered absence costs no round trip", got)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty for an unknown identity, got %v", files)
	}
}

// An unreachable forge is a miss that stores nothing; a later request fetches
// again rather than serving a poisoned entry.
func TestServeUnreachableForgeStoresNothingAndRecovers(t *testing.T) {
	f := newForgeStub(t)
	dir := t.TempDir()
	p, err := New(Config{ForgeURL: "http://127.0.0.1:1", CacheDir: dir})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	rec := get(p, "alice")
	if rec.Code == http.StatusOK {
		t.Fatal("an unreachable forge must be a miss, got 200")
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Fatalf("store must be empty after an unreachable miss, got %v", files)
	}

	p2, _ := newProxy(t, f)
	if rec := get(p2, "alice"); rec.Code != http.StatusOK {
		t.Fatalf("after recovery: status = %d", rec.Code)
	}
}

// A cold store hit by concurrent viewers of the same login must fetch once,
// not stampede the forge one round trip per viewer.
func TestServeConcurrentMissesFetchOnce(t *testing.T) {
	f := newForgeStub(t)
	gate := make(chan struct{})
	inFlight := make(chan struct{}, 1)
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			select {
			case inFlight <- struct{}{}:
			default: // a second round trip: let it through and fail the count below
			}
			<-gate // hold the first fetch until the queue has built up
		}
		orig.ServeHTTP(w, r)
	})
	p, _ := newProxy(t, f)

	var wg sync.WaitGroup
	codes := make([]int, 8)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = get(p, "alice").Code
		}()
	}
	<-inFlight // the fetch is under way; the others are queued behind it or will find the render
	close(gate)
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("viewer %d: status = %d, want 200", i, code)
		}
	}
	if got := f.apiHits.Load(); got != 1 {
		t.Errorf("API hits = %d, want 1 (one round trip for eight viewers)", got)
	}
}

// Deduplicating a login's fetches must not serialise the route: a cold store
// holding N identities warms them at once, and one hung request stalls the
// login it belongs to rather than every tile on the page.
func TestServeFetchesDifferentLoginsInParallel(t *testing.T) {
	f := newForgeStub(t)
	var mu sync.Mutex
	arrived := map[string]bool{}
	both := make(chan struct{})
	var serialised atomic.Bool
	var once sync.Once
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			mu.Lock()
			arrived[strings.TrimPrefix(r.URL.Path, "/api/v1/users/")] = true
			if len(arrived) == 2 {
				once.Do(func() { close(both) })
			}
			mu.Unlock()
			// Hold each round trip until the other login is in flight too.
			select {
			case <-both:
			case <-time.After(2 * time.Second):
				serialised.Store(true) // the other login never got as far as the forge
			}
		}
		orig.ServeHTTP(w, r)
	})
	p, _ := newProxy(t, f)

	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i, login := range []string{"alice", "bob"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = get(p, login).Code
		}()
	}
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("login %d: status = %d, want 200", i, code)
		}
	}
	if serialised.Load() {
		t.Error("the second login never reached the forge while the first was in flight: one fetch holds up every other login")
	}
}

// The issue's last verification case: a render that cannot be renamed into
// place is a miss, not corruption. Here the entry's place is held by a
// directory, so the store read misses, the fetch runs, and the rename fails.
func TestServeFailedRenameIsAMissAndKeepsWhatWasThere(t *testing.T) {
	f := newForgeStub(t)
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "alice.png"), 0o755); err != nil {
		t.Fatalf("place the entry: %v", err)
	}
	kept := filepath.Join(dir, "alice.png", "render")
	if err := os.WriteFile(kept, []byte("previous render"), 0o644); err != nil {
		t.Fatalf("place the render: %v", err)
	}
	p, err := New(Config{ForgeURL: f.srv.URL, CacheDir: dir})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	if code := get(p, "alice").Code; code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when the render cannot be renamed into place", code)
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "alice.png" {
		t.Errorf("store = %v, want no temp file left beside what was already there", names)
	}
	survivor, err := os.ReadFile(kept)
	if err != nil || string(survivor) != "previous render" {
		t.Errorf("what the entry held = %q (%v), want it left intact", survivor, err)
	}
}

// The other half of the discipline: a store that cannot even take the temp
// file is a miss, and says nothing by leaving a partial file behind. The path
// is a regular file rather than a directory made read-only, so CreateTemp
// fails for every uid instead of only for the non-root ones.
func TestServeFailedTempCreateIsAMissAndStoresNothing(t *testing.T) {
	f := newForgeStub(t)
	dir := t.TempDir()
	blocked := filepath.Join(dir, "store")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("place the store path: %v", err)
	}
	p, err := New(Config{ForgeURL: f.srv.URL, CacheDir: dir})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}
	p.cacheDir = blocked

	if code := get(p, "alice").Code; code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when no temp file can be created", code)
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "store" {
		t.Errorf("store = %v, want only the path this test placed there", names)
	}
}

// Nothing in the suite is square by construction, but every source so far
// was: fitting and centring only show themselves on a landscape source, where
// stretching the source to the full square would put colour where the
// letterboxing belongs. This is the case a doc comment promises.
func TestServeFitsAndCentresALandscapeSource(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageBody = encodePNG(t, 592, 300)
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	if code := get(p, "alice").Code; code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a landscape source", code)
	}
	stored, err := os.ReadFile(filepath.Join(dir, "alice.png"))
	if err != nil {
		t.Fatalf("stored render missing: %v", err)
	}
	img, err := png.Decode(bytes.NewReader(stored))
	if err != nil {
		t.Fatalf("stored render is not a PNG: %v", err)
	}
	if b := img.Bounds(); b.Dx() != DefaultTileSize || b.Dy() != DefaultTileSize {
		t.Fatalf("stored render = %dx%d, want %dx%d", b.Dx(), b.Dy(), DefaultTileSize, DefaultTileSize)
	}
	// 592 is the edge that fits the tile exactly, so 300 lands on 32 rows
	// centred at y=16..47: transparent above and below, opaque inside.
	for _, y := range []int{8, 56} {
		if _, _, _, a := img.At(0, y).RGBA(); a != 0 {
			t.Errorf("alpha at y=%d = %d, want 0: the source was stretched, not fitted", y, a)
		}
	}
	if _, _, _, a := img.At(0, 32).RGBA(); a == 0 {
		t.Error("alpha at the centre row = 0, want the fitted source there")
	}
}

// Every content type the proxy accepts must actually decode and land on the
// canonical PNG render, and the table has to move with the map it covers: an
// entry in acceptedImageTypes that no test fetches is an entry that cannot
// fail a test.
func TestServeAcceptsEveryAcceptedImageType(t *testing.T) {
	encoders := map[string]func(t *testing.T) []byte{
		"image/png":  func(t *testing.T) []byte { return encodePNG(t, 309, 309) },
		"image/jpeg": func(t *testing.T) []byte { return encodeJPEG(t, 400, 400) },
		"image/gif":  func(t *testing.T) []byte { return encodeGIF(t, 400, 400) },
	}
	if len(encoders) != len(acceptedImageTypes) {
		t.Fatalf("acceptedImageTypes holds %d entries, the table covers %d", len(acceptedImageTypes), len(encoders))
	}

	for mediaType, encode := range encoders {
		t.Run(mediaType, func(t *testing.T) {
			if !acceptedImageTypes[mediaType] {
				t.Fatalf("%q is no longer accepted, so this source now has no route", mediaType)
			}
			f := newForgeStub(t)
			f.mu.Lock()
			f.imageType, f.imageBody = mediaType, encode(t)
			f.mu.Unlock()
			p, dir := newProxy(t, f)

			rec := get(p, "alice")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d (body %q)", rec.Code, rec.Body.String())
			}
			stored, err := os.ReadFile(filepath.Join(dir, "alice.png"))
			if err != nil {
				t.Fatalf("stored render missing: %v", err)
			}
			if _, err := png.Decode(bytes.NewReader(stored)); err != nil {
				t.Errorf("stored render is not a PNG: %v", err)
			}
		})
	}
}

// A miss is remembered for negativeTTL: the route is unauthenticated, so
// without that memory every inbound request for an unknown or transiently
// broken login buys a fresh pair of requests on the forge, paid for with
// hotelier's own client certificate.
func TestServeRemembersUnknownIdentityBetweenRequests(t *testing.T) {
	f := newForgeStub(t)
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			f.apiHits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		orig.ServeHTTP(w, r)
	})
	p, dir := newProxy(t, f)

	first := get(p, "ghost")
	second := get(p, "ghost")

	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d and %d, want 200 both times: the glyph is a cacheable answer",
			first.Code, second.Code)
	}
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Error("the remembered absence changed glyph between requests")
	}
	if got := f.apiHits.Load(); got != 1 {
		t.Errorf("API hits = %d, want 1: a remembered miss costs no round trip", got)
	}
	if files := storeFiles(t, dir); len(files) != 0 {
		t.Errorf("store must be empty after misses, got %v", files)
	}
}

// The memory covers an unreachable or misbehaving forge as readily as an
// unknown identity, so a slow upstream is re-probed once per window rather than
// once per page render.
func TestServeRemembersAnUpstreamFailure(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.apiStatus = http.StatusBadRequest
	f.mu.Unlock()
	p, _ := newProxy(t, f)

	first := get(p, "alice")
	second := get(p, "alice")

	if first.Code != http.StatusBadGateway || second.Code != http.StatusBadGateway {
		t.Fatalf("statuses = %d and %d, want 502 both times", first.Code, second.Code)
	}
	if got := f.apiHits.Load(); got != 1 {
		t.Errorf("API hits = %d, want 1", got)
	}
}

// A request whose own context was cancelled is this server's business, not
// the forge's: an <img> a dashboard dropped mid-load, a Shutdown. Ending that
// fetch must not write a minute of 502s over a healthy login for everyone
// else, so the abandoned request keeps its miss to itself.
func TestServeACancelledRequestIsNotRemembered(t *testing.T) {
	f := newForgeStub(t)
	arrived, _ := parkFirstAPIRoundTrip(t, f)
	p, dir := newProxy(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil).WithContext(ctx))
		result <- rec
	}()
	awaitArrival(t, arrived)
	cancel() // the re-render closed the connection this request rode on
	rec := <-result
	if rec.Code == http.StatusOK {
		t.Fatal("an abandoned request was answered with a tile, want its own miss")
	}

	// The next viewer of that same healthy login must pay for the round trip
	// the first one walked away from, not inherit its failure.
	if code := get(p, "alice").Code; code != http.StatusOK {
		t.Fatalf("the next viewer got %d with %d API hits: an abandoned request was remembered as an upstream failure",
			code, f.apiHits.Load())
	}
	if files := storeFiles(t, dir); len(files) != 1 || files[0] != "alice.png" {
		t.Errorf("store = %v, want the tile the second request fetched", files)
	}
}

// parkFirstAPIRoundTrip parks the stub's first API request until release is
// called or that request's own context ends, and signals when it arrives. A
// test can then be certain a fetch is in flight — the login's flight is
// registered before the round trip starts and retired only when it ends — and
// can decide what becomes of it. A test that ends the parked request by other
// means can ignore release: the parked round trip ends when the request's own
// context does, as it always eventually would.
func parkFirstAPIRoundTrip(t *testing.T, f *forgeStub) (arrived chan struct{}, release func()) {
	t.Helper()
	arrived = make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(releaseCh) }) }
	var mu sync.Mutex
	var parked bool
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			mu.Lock()
			first := !parked
			parked = true
			mu.Unlock()
			if first {
				arrived <- struct{}{}
				select {
				case <-releaseCh:
				case <-r.Context().Done():
				}
			}
		}
		orig.ServeHTTP(w, r)
	})
	return arrived, release
}

// awaitArrival blocks until a parked round trip says it is under way. The wait is
// bounded so that a fetch which never reached the stub fails the test rather than
// hanging it.
func awaitArrival(t *testing.T, arrived chan struct{}) {
	t.Helper()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the fetch never reached the stub")
	}
}

// awaitAPIHits blocks until the stub has counted n API requests. A parked round
// trip counts its hit only when it resumes — which can be after its client has
// given up on it — so a test reading the tally after a parked fetch waits for
// that round trip to finish rather than assume it already has.
func awaitAPIHits(t *testing.T, f *forgeStub, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for int(f.apiHits.Load()) < n {
		select {
		case <-deadline:
			t.Fatalf("API hits = %d after 5s, want %d", f.apiHits.Load(), n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// A viewer queued behind a fetch its leader abandoned looks again rather than
// inheriting a miss nobody was answered: one aborted page render must not
// leave every other viewer of a healthy login with a minute of 502s. The leader is on a deadline,
// which is also the room the queued viewers have to reach the wait they are meant
// to be in, and nothing waits on that clock: the test waits on the stub's signal
// and then on the leader's own response.
func TestServeViewerQueuedBehindAnAbandonedFetchDoesNotInheritItsMiss(t *testing.T) {
	f := newForgeStub(t)
	arrived, _ := parkFirstAPIRoundTrip(t, f)
	p, dir := newProxy(t, f)

	leaderCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	leader := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil).WithContext(leaderCtx))
		leader <- rec.Code
	}()
	// The leader's fetch is under way, so its flight is on the map and any
	// viewer started now can only queue behind it.
	awaitArrival(t, arrived)

	const viewers = 4
	codes := make([]int, viewers)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = get(p, "alice").Code
		}()
	}

	leaderCode := <-leader
	wg.Wait()
	if leaderCode == http.StatusOK {
		t.Fatal("the abandoned leader was answered with a tile, want its own miss")
	}
	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("viewer %d: status = %d, want 200: a queued viewer inherited an abandoned fetch's miss", i, code)
		}
	}
	if got := f.apiHits.Load(); got != 2 {
		t.Errorf("API hits = %d, want 2: the leader's aborted round trip and one fetch the viewers shared", got)
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "alice.png" {
		t.Errorf("store = %v, want the tile the viewers fetched once", names)
	}
}

// The other exit a queued viewer has is leaving itself. That must not answer from
// the fetch it walked away from — which never got an answer to hand on — and it
// must not stop that fetch either: the leader finishes for whoever is still
// there, and the next viewer finds the render on disk.
func TestServeViewerWhoLeavesKeepsItsOwnMissAndTheFetchCarriesOn(t *testing.T) {
	f := newForgeStub(t)
	// release lets the parked round trip go, whether the test reaches the point
	// of wanting it or fails before then and leaves the cleanup to do it.
	arrived, release := parkFirstAPIRoundTrip(t, f)
	defer release()
	p, dir := newProxy(t, f)

	leader := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil))
		leader <- rec.Code
	}()
	awaitArrival(t, arrived) // the leader is in its round trip, so the next viewer can only queue

	// This viewer's connection is already closed by the time it reaches the
	// wait: a browser that gave up on an <img> before its tile came back.
	leftCtx, cancelLeft := context.WithCancel(context.Background())
	cancelLeft()
	left := httptest.NewRecorder()
	p.ServeHTTP(left, httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil).WithContext(leftCtx))
	if left.Code != http.StatusBadGateway {
		t.Fatalf("the viewer who left got %d, want 502: absence was claimed where nothing was answered", left.Code)
	}

	release()
	if code := <-leader; code != http.StatusOK {
		t.Fatalf("leader: status = %d, want 200: one viewer leaving broke the fetch it shared", code)
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "alice.png" {
		t.Fatalf("store = %v, want the render the leader stored", names)
	}
	if code := get(p, "alice").Code; code != http.StatusOK {
		t.Fatalf("the next viewer: status = %d, want 200 from the render the leader finished", code)
	}
	if got := f.apiHits.Load(); got != 1 {
		t.Errorf("API hits = %d, want 1: a viewer leaving must not cost another round trip", got)
	}
}

// Whose clock ran out decides what a miss says about the identity. The client's
// timeout firing on a forge that never answered is a fact about the forge, and
// the memory exists so a degraded upstream is probed once per window rather than
// once per inbound request; a deadline on the viewer's own request is not a
// verdict at all, and the next viewer of that login still gets a fetch.
func TestServeRemembersAMissUnlessTheViewerStoppedIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		// exactly one of these is the clock that expires
		clientTimeout    time.Duration
		viewerDeadline   time.Duration
		wantFirst        int
		wantAPIHitsAfter int
		wantSecond       int
	}{
		{
			name:             "this client timed out on a forge that never answered",
			clientTimeout:    250 * time.Millisecond,
			wantFirst:        http.StatusBadGateway,
			wantAPIHitsAfter: 1,
			wantSecond:       http.StatusBadGateway,
		},
		{
			name:             "the viewer's own request ran out of time",
			viewerDeadline:   250 * time.Millisecond,
			wantFirst:        http.StatusBadGateway,
			wantAPIHitsAfter: 2,
			wantSecond:       http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForgeStub(t)
			arrived, _ := parkFirstAPIRoundTrip(t, f)
			p, _ := newProxy(t, f)
			if tc.clientTimeout > 0 {
				// This row is about that timeout, so the test shortens the clock New
				// sets rather than waiting 15s on it.
				p.client.Timeout = tc.clientTimeout
			}

			ctx := context.Background()
			if tc.viewerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.viewerDeadline)
				defer cancel()
			}
			req := httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil).WithContext(ctx)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				rec := httptest.NewRecorder()
				p.ServeHTTP(rec, req)
				done <- rec
			}()
			awaitArrival(t, arrived)
			// However the first request ends, it is over before the second is made:
			// whichever clock expires cancels the round trip the stub is parked on,
			// and the parked handler returns on that.
			first := <-done
			if first.Code != tc.wantFirst {
				t.Fatalf("first status = %d, want %d", first.Code, tc.wantFirst)
			}

			if code := get(p, "alice").Code; code != tc.wantSecond {
				t.Errorf("second status = %d, want %d", code, tc.wantSecond)
			}
			awaitAPIHits(t, f, tc.wantAPIHitsAfter)
			if got := int(f.apiHits.Load()); got != tc.wantAPIHitsAfter {
				t.Errorf("API hits = %d, want %d", got, tc.wantAPIHitsAfter)
			}
		})
	}
}

// Remembering is not caching a verdict: once the window closes the identity is
// looked up again, so a login created or repaired after the miss gets its face
// without a human touching the store.
func TestServeFetchesAgainOnceAMissExpires(t *testing.T) {
	f := newForgeStub(t)
	orig := f.srv.Config.Handler
	var known atomic.Bool
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") && !known.Load() {
			f.apiHits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		orig.ServeHTTP(w, r)
	})
	p, _ := newProxy(t, f)

	absent := get(p, "alice")
	if absent.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with the glyph", absent.Code)
	}
	expireMisses(p)
	known.Store(true)
	present := get(p, "alice")
	if present.Code != http.StatusOK {
		t.Fatalf("status after the window = %d, want 200", present.Code)
	}
	if bytes.Equal(present.Body.Bytes(), absent.Body.Bytes()) {
		t.Error("the identity got its lookup back but is still served the glyph")
	}
	if got := f.apiHits.Load(); got != 2 {
		t.Errorf("API hits = %d, want 2 (the miss was not permanent)", got)
	}
}

// The memory is bounded: an attacker inventing logins must not convert the
// forge's load into an unbounded allocation here either.
func TestNegativeStoreStaysBounded(t *testing.T) {
	p, _ := newProxy(t, newForgeStub(t))

	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range negativeMaxEntries + 64 {
		p.rememberMiss(fmt.Sprintf("user%06d", i), http.StatusNotFound)
	}
	if len(p.neg) > negativeMaxEntries {
		t.Fatalf("negative store holds %d entries, want at most %d", len(p.neg), negativeMaxEntries)
	}
}

// expireMisses ages every remembered miss out, standing in for the window
// closing without the test waiting on a wall clock.
func expireMisses(p *Proxy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for login, m := range p.neg {
		m.expires = time.Now().Add(-time.Second)
		p.neg[login] = m
	}
}

// A miss says why in the journal: an expired client certificate, an ingress
// answering 400 with HTML and a read-only cache directory must not read alike
// to whoever is asked why the dashboard has no faces. The response stays the
// same short line, and a miss that was only remembered says nothing again.
func TestServeLogsTheCauseOfAMiss(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.apiStatus = http.StatusBadRequest
	f.mu.Unlock()

	var mu sync.Mutex
	var lines []string
	p, err := New(Config{
		ForgeURL: f.srv.URL,
		CacheDir: t.TempDir(),
		Logf: func(format string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, fmt.Sprintf(format, args...))
		},
	})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	if code := get(p, "alice").Code; code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	mu.Lock()
	logged := strings.Join(lines, "\n")
	mu.Unlock()
	if !strings.Contains(logged, "alice") || !strings.Contains(logged, "400") {
		t.Errorf("log = %q, want the login and the upstream cause", logged)
	}

	mu.Lock()
	before := len(lines)
	mu.Unlock()
	get(p, "alice")
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != before {
		t.Errorf("a remembered miss logged %d further lines, want 0", len(lines)-before)
	}
}

// A client certificate over a plain http forge_url would be dropped without a
// word, so the pairing is refused rather than quietly degraded to an
// unauthenticated hop.
func TestNewRefusesClientCertificateOverPlainHTTP(t *testing.T) {
	_, err := New(Config{
		ForgeURL:        "http://forge.example.com",
		CacheDir:        t.TempDir(),
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{{}}},
	})
	if err == nil {
		t.Fatal("New with a cert pair over http succeeded, want an error")
	}
}

// New refuses a ForgeURL that cannot be an absolute http(s) origin: a proxy
// that could never reach the forge must not start in a half-life.
func TestNewRejectsUnusableForgeURL(t *testing.T) {
	for _, u := range []string{"", "not a url", "ftp://forge.example.com"} {
		if _, err := New(Config{ForgeURL: u, CacheDir: t.TempDir()}); err == nil {
			t.Errorf("New(%q) succeeded, want an error", u)
		}
	}
}

// ---------------------------------------------------------------------------
// Cache policy and minted validators (issue #276).
// ---------------------------------------------------------------------------

// etagFor is the validator the proxy must mint: a strong content hash of the
// exact bytes it serves. Upstream validators are never forwarded, so what the
// proxy emits can never equal one.
func etagFor(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// ageEntry moves a stored render past max-age, standing in for a day of
// dashboard renders without the test waiting on a wall clock.
func ageEntry(t *testing.T, dir, login string, age time.Duration) {
	t.Helper()
	path := filepath.Join(dir, login+".png")
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("age stored render: %v", err)
	}
}

// waitFor polls a condition the proxy drives from a background goroutine, so
// no test waits on a guessed delay.
func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for !done() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// The proxy's own cache policy, exactly: one day fresh, a week servable with
// background refresh, a month on error — and must-revalidate nowhere. The
// validator it presents is minted from the served bytes: the upstream sends
// Last-Modified, a private cache hint, and (in the planted case) an ETag, and
// none of it may appear in the proxy's answer.
func TestServeSendsCachePolicyAndMintedETag(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageLastModified = time.Date(2026, time.November, 9, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	f.imageETag = `"upstream-validator"`
	f.mu.Unlock()
	p, _ := newProxy(t, f)

	rec := get(p, "alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q)", rec.Code, rec.Body.String())
	}

	if got := rec.Header().Get("Cache-Control"); got != cacheControlValue {
		t.Errorf("Cache-Control = %q, want %q", got, cacheControlValue)
	}
	etag := rec.Header().Get("ETag")
	if want := etagFor(rec.Body.Bytes()); etag != want {
		t.Errorf("ETag = %s, want the content hash %s", etag, want)
	}
	if etag == `"upstream-validator"` {
		t.Error("the upstream's ETag was served in place of a minted validator")
	}
	if lm := rec.Header().Get("Last-Modified"); lm != "" {
		t.Errorf("Last-Modified = %q, want none: the upstream validator is only carried on the proxy's own conditional requests", lm)
	}
}

// A minted validator that nothing honours is decoration: a viewer that
// already holds these bytes gets a 304 and no body.
func TestServeAnswersMatchingIfNoneMatchWith304(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("the first response carried no ETag to revalidate with")
	}
	hits := f.totalHits()

	// An hour, still inside max-age: nothing revalidates behind the request,
	// so what the assertion sees below is this answer and nothing else. A
	// conditional answer still has to say how old the bytes are (RFC 9110
	// §5.1) — the ETag alone says which face is held, not since when.
	ageEntry(t, dir, "alice", time.Hour)

	req := httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil)
	req.Header.Set("If-None-Match", etag)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("304 carried a %d-byte body, want none", rec.Body.Len())
	}
	if got := rec.Header().Get("ETag"); got != etag {
		t.Errorf("304 ETag = %q, want it to stand as it was", etag)
	}
	if got, err := strconv.Atoi(rec.Header().Get("Age")); err != nil || got < 3540 || got > 3660 {
		t.Errorf("304 Age = %q, want about 3600: these bytes are an hour old", rec.Header().Get("Age"))
	}
	if got := f.totalHits(); got != hits {
		t.Errorf("forge hits grew to %d on a 304, want %d", got, hits)
	}
}

// stale-while-revalidate, the server's own half: past max-age the stored
// render answers at once and a background revalidation refreshes it. The
// stale answer must not wait on the forge, and one stale entry must trigger
// exactly one refresh.
func TestStaleRenderServesImmediatelyAndRevalidatesInBackground(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}

	// The identity's face changes on the forge, and the entry passes max-age.
	f.mu.Lock()
	f.imageBody = encodePNG(t, 300, 300)
	f.mu.Unlock()
	ageEntry(t, dir, "alice", maxAge+time.Hour)

	stale := get(p, "alice")
	if stale.Code != http.StatusOK || !bytes.Equal(stale.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("the stale request did not serve the stored render at once")
	}

	awaitAPIHits(t, f, 2)
	waitFor(t, "the revalidated render to land", func() bool {
		stored, err := os.ReadFile(filepath.Join(dir, "alice.png"))
		return err == nil && !bytes.Equal(stored, first.Body.Bytes())
	})

	refreshed := get(p, "alice")
	if bytes.Equal(refreshed.Body.Bytes(), first.Body.Bytes()) {
		t.Error("the background revalidation never reached the render")
	}
	if got := f.apiHits.Load(); got != 2 {
		t.Errorf("API hits = %d, want 2: one stale entry triggers one refresh", got)
	}
}

func TestServeStampsTheAgeOfAStaleRender(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	fresh := get(p, "alice")
	if fresh.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", fresh.Code)
	}
	if got := fresh.Header().Get("Age"); got != "" {
		t.Errorf("Age = %q on a render the forge just handed over, want none", got)
	}

	age := maxAge + time.Hour
	ageEntry(t, dir, "alice", age)
	stale := get(p, "alice")
	if stale.Code != http.StatusOK {
		t.Fatalf("stale: status = %d", stale.Code)
	}
	got, err := strconv.Atoi(stale.Header().Get("Age"))
	if err != nil {
		t.Fatalf("Age = %q, want the seconds these bytes have been the entry", stale.Header().Get("Age"))
	}
	if want := int(age.Seconds()); got < want-60 || got > want+60 {
		t.Errorf("Age = %d, want about %d", got, want)
	}

	// The same request asked the forge once, as any stale answer does.
	awaitAPIHits(t, f, 2)
	waitFor(t, "the revalidation to settle", func() bool {
		info, err := os.Stat(filepath.Join(dir, "alice.png"))
		return err == nil && time.Since(info.ModTime()) < time.Hour
	})
}

// A render whose mtime lies ahead of the clock — an NTP correction that stepped
// the clock back after boot, a store copied off a host that was running fast —
// must not read as brand new. A negative age clears every window the store
// checks at once: no Age, no revalidation, no outer bound, and no eviction path
// either, until real time catches up with a timestamp the store did not pick.
// So the clock is clamped to just past max-age: the render still answers this
// request, and the forge is asked behind it.
func TestFutureDatedRenderIsServedButNeverAsAFreshOne(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}
	ageEntry(t, dir, "alice", -time.Hour) // the mtime is an hour ahead of the clock

	rec := get(p, "alice")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("status = %d, want the render to keep answering while the clock is odd", rec.Code)
	}
	if got := rec.Header().Get("Age"); got == "" {
		t.Error("Age = none, want an answer that does not claim these bytes are freshly minted")
	}
	awaitAPIHits(t, f, 2)
}

// The store obeys the staleness windows the response advertises. RFC 9110 §4.2.4
// lets a cache use a stale response while revalidating and on error, and no
// further, so past max-age plus stale-if-error the request takes the forge path
// instead of the file's — a bodyless 502 with the forge down, which is where
// stale-if-error belongs: honoured by the caches downstream of here, keeping the
// faces they already hold rather than this one. Both directions of that fall-
// through are pinned: what the store refuses, and where the request then lands.

func TestServeRefusesAStaleRenderBeyondThePolicyWindows(t *testing.T) {
	t.Run("past the windows, the forge path is taken", func(t *testing.T) {
		f := newForgeStub(t)
		p, dir := newProxy(t, f)

		first := get(p, "alice")
		if first.Code != http.StatusOK {
			t.Fatalf("warm-up: status = %d", first.Code)
		}
		ageEntry(t, dir, "alice", maxStaleAge+time.Hour)
		f.srv.Close() // the forge went down while this entry aged past the policy

		rec := get(p, "alice")
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502: a render past the windows this proxy advertises still answered from the store", rec.Code)
		}
		if bytes.HasPrefix(rec.Body.Bytes(), []byte{0x89, 'P', 'N', 'G'}) {
			t.Error("the body was the stale tile, want the caller's own miss line")
		}
	})

	// The half the README and config/server.example.yaml promise: refusing an
	// over-aged render is a fall-through to the forge, not a dead end. An entry
	// that simply aged out while the forge stayed healthy comes back current —
	// nothing revalidates an entry the store has refused, so a build that
	// answered 502 here instead would leave an aged-out avatar 502 forever.
	t.Run("past the windows, a healthy forge answers it", func(t *testing.T) {
		f := newForgeStub(t)
		p, dir := newProxy(t, f)

		first := get(p, "alice")
		if first.Code != http.StatusOK {
			t.Fatalf("warm-up: status = %d", first.Code)
		}
		f.mu.Lock()
		f.imageBody = encodePNG(t, 300, 300) // the face changed while this entry aged out
		f.mu.Unlock()
		ageEntry(t, dir, "alice", maxStaleAge+time.Hour)

		rec := get(p, "alice")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d (body %q), want 200 from the forge path", rec.Code, rec.Body.String())
		}
		if bytes.Equal(rec.Body.Bytes(), first.Body.Bytes()) {
			t.Error("the over-aged render was answered from the store instead of the forge")
		}
		if got := rec.Header().Get("Age"); got != "" {
			t.Errorf("Age = %q on bytes that came off the forge path, want none", got)
		}
		if stored, err := os.ReadFile(filepath.Join(dir, "alice.png")); err != nil || !bytes.Equal(stored, rec.Body.Bytes()) {
			t.Errorf("the entry was not rewritten: stored=%v err=%v", stored != nil, err)
		}
		if got := f.apiHits.Load(); got != 2 {
			t.Errorf("API hits = %d, want 2: one refused render is one trip to the forge", got)
		}
	})
}

// A forge that fails fast must not turn every request for a stale entry into one
// forge hop: the memory of a recent miss gates this background path as it gates
// the foreground one, so a degraded window costs one round trip per identity per
// window. The flights map cannot do that — it deduplicates only fetches still in
// progress, and an instant error defeats it.

func TestStaleRevalidationIsRateBoundedByARememberedMiss(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}
	ageEntry(t, dir, "alice", maxAge+time.Hour)
	f.mu.Lock()
	f.apiStatus = http.StatusBadRequest // the ingress answers at once, in error
	f.mu.Unlock()

	stale := get(p, "alice")
	if stale.Code != http.StatusOK || !bytes.Equal(stale.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("the stale request did not serve the stored render at once")
	}
	awaitAPIHits(t, f, 2)
	waitFor(t, "the failed revalidation to be remembered", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		m, ok := p.neg["alice"]
		return ok && time.Now().Before(m.expires)
	})

	for i := range 4 {
		rec := get(p, "alice")
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), first.Body.Bytes()) {
			t.Fatalf("request %d during the degraded window: status = %d, want the stored tile", i, rec.Code)
		}
		// Quiescence, not a sleep: without draining, these four requests land
		// inside one round trip and the flights map deduplicates three of them,
		// so the tally would hold even with no gate on this path at all.
		awaitQuiescence(t, p)
	}
	if got := f.apiHits.Load(); got != 2 {
		t.Errorf("API hits = %d, want 2: a stale entry in a forge-degraded window must cost one round trip per window, not one per inbound request", got)
	}

	// And the gate opens when the window closes: once the remembered miss has
	// expired, the next request pays for a round trip again.
	expireMisses(p)
	if rec := get(p, "alice"); rec.Code != http.StatusOK {
		t.Fatalf("after the window closed: status = %d, want the stored tile", rec.Code)
	}
	awaitAPIHits(t, f, 3)
}

// awaitQuiescence waits for the proxy's background work to drain, so a request
// is answered while no fetch is in flight and the assertion that follows is a
// statement about rate rather than about overlap.
func awaitQuiescence(t *testing.T, p *Proxy) {
	t.Helper()
	waitFor(t, "background revalidation to quiesce", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.flights) == 0
	})
}

// The sidecar only sharpens the next revalidation's If-Modified-Since, so a
// render already renamed into place must not be reported as a miss because the
// sidecar could not follow it — the asymmetry forget already applies to it. The
// render is what gets served; the sidecar is provenance.

// The outage half of stale-if-error: an unreachable forge may not overwrite a
// stored face. The request is answered from the store, the cache stays as it
// was, and the next viewer is answered from it too.
func TestUnreachableRevalidationKeepsTheStoredTileServing(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}
	ageEntry(t, dir, "alice", maxAge+time.Hour)
	f.srv.Close() // the forge goes down after the render was stored

	for i := range 2 {
		rec := get(p, "alice")
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), first.Body.Bytes()) {
			t.Fatalf("request %d during the outage: status = %d, want the stored tile", i, rec.Code)
		}
	}
	if names := storeFiles(t, dir); len(names) != 1 || names[0] != "alice.png" {
		t.Errorf("store = %v, want the outage to leave the cache untouched", names)
	}
}

// The upstream Last-Modified is forwarded in exactly one place: If-Modified-
// Since on the proxy's own revalidation. A 304 verdict winds the entry's
// clock without rewriting the tile, so a render that has not changed does not
// re-ask once a day for a copy it already holds.
func TestRevalidationCarriesTheStoredLastModified(t *testing.T) {
	f := newForgeStub(t)
	lastModified := time.Date(2026, time.November, 9, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	f.mu.Lock()
	f.imageLastModified = lastModified
	f.honorIfModSince = true
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}
	f.mu.Lock()
	if got := f.ifModSince; got != "" {
		t.Fatalf("the cold fetch carried If-Modified-Since %q, want a plain GET", got)
	}
	f.mu.Unlock()

	ageEntry(t, dir, "alice", maxAge+time.Hour)
	stale := get(p, "alice")
	if stale.Code != http.StatusOK || !bytes.Equal(stale.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("the stale request did not serve the stored render at once")
	}

	awaitAPIHits(t, f, 2)
	waitFor(t, "the revalidating image request to carry If-Modified-Since", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.ifModSince == lastModified
	})

	// The 304 refreshes the entry's clock and touches nothing else.
	waitFor(t, "the revalidation to settle", func() bool {
		info, err := os.Stat(filepath.Join(dir, "alice.png"))
		return err == nil && time.Since(info.ModTime()) < time.Hour
	})
	if got := get(p, "alice"); !bytes.Equal(got.Body.Bytes(), first.Body.Bytes()) {
		t.Error("the unchanged render was rewritten instead of kept")
	}
	if got := f.apiHits.Load(); got != 2 {
		t.Errorf("API hits = %d, want 2: a 304 ends the revalidation, it does not restart it daily", got)
	}
	names := storeFiles(t, dir)
	if len(names) != 2 || names[1] != "alice.png" {
		t.Errorf("store = %v, want the render beside its one sidecar", names)
	}
}

// A definitive 404 during revalidation says the face is gone for good — a
// deleted account must converge on the glyph rather than serve an old face
// through stale-if-error forever. An error keeps the face (see the outage
// test above); absence drops it.
func TestRevalidationDropsTheRenderOfADeletedIdentity(t *testing.T) {
	f := newForgeStub(t)
	p, dir := newProxy(t, f)

	first := get(p, "alice")
	if first.Code != http.StatusOK {
		t.Fatalf("warm-up: status = %d", first.Code)
	}

	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			f.apiHits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		orig.ServeHTTP(w, r)
	})

	ageEntry(t, dir, "alice", maxAge+time.Hour)
	stale := get(p, "alice")
	if stale.Code != http.StatusOK || !bytes.Equal(stale.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("the stale request did not serve the stored render at once")
	}

	awaitAPIHits(t, f, 2)
	waitFor(t, "the deleted identity's render to leave the store", func() bool {
		return len(storeFiles(t, dir)) == 0
	})

	glyph := get(p, "alice")
	if glyph.Code != http.StatusOK || bytes.Equal(glyph.Body.Bytes(), first.Body.Bytes()) {
		t.Errorf("the deleted identity is still served %d with its old face, want the glyph", glyph.Code)
	}
}

func TestServeServesTheRenderWhenOnlyTheSidecarWriteFails(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageLastModified = time.Date(2026, time.November, 9, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	f.mu.Unlock()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "alice.last-modified"), 0o755); err != nil {
		t.Fatalf("block the sidecar's place: %v", err)
	}
	var logMu sync.Mutex
	var lines []string
	p, err := New(Config{
		ForgeURL: f.srv.URL,
		CacheDir: dir,
		Logf: func(format string, args ...any) {
			logMu.Lock()
			defer logMu.Unlock()
			lines = append(lines, fmt.Sprintf(format, args...))
		},
	})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	rec := get(p, "alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q), want 200 from the render that did land", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("ETag"); got != etagFor(rec.Body.Bytes()) {
		t.Errorf("ETag = %q, want the content hash of the served render", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "alice.png")); err != nil {
		t.Errorf("stored render missing: %v", err)
	}
	logMu.Lock()
	logged := strings.Join(lines, "\n")
	logMu.Unlock()
	if !strings.Contains(logged, "alice") || !strings.Contains(logged, "last-modified") {
		t.Errorf("log = %q, want the render's identity and the sidecar that could not be written", logged)
	}
}

// "must-revalidate appears nowhere in emitted headers" — checked against every
// shape of answer the route makes, not just the tile.
func TestNoEmittedHeaderCarriesMustRevalidate(t *testing.T) {
	f := newForgeStub(t)
	p, _ := newProxy(t, f)
	unreachable, err := New(Config{ForgeURL: "http://127.0.0.1:1", CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	wrongMethod := httptest.NewRecorder()
	p.ServeHTTP(wrongMethod, httptest.NewRequest(http.MethodPost, PathPrefix+"alice", nil))
	badLogin := httptest.NewRecorder()
	p.ServeHTTP(badLogin, httptest.NewRequest(http.MethodGet, PathPrefix+"not%20a%20login", nil))

	answers := map[string]*httptest.ResponseRecorder{
		"invalid login":     badLogin,
		"wrong method":      wrongMethod,
		"unreachable forge": get(unreachable, "alice"),
		"tile":              get(p, "alice"),
	}
	tile := answers["tile"]
	conditional := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil)
	req.Header.Set("If-None-Match", tile.Header().Get("ETag"))
	p.ServeHTTP(conditional, req)
	answers["304"] = conditional

	for name, rec := range answers {
		for header, values := range rec.Header() {
			for _, v := range values {
				if strings.Contains(v, "must-revalidate") {
					t.Errorf("%s: header %s = %q carries must-revalidate", name, header, v)
				}
			}
		}
		if !strings.Contains(strings.ToLower(name), "304") && rec.Body.Len() == 0 {
			t.Errorf("%s: empty body, want the sweep to have exercised a real answer", name)
		}
	}
}
