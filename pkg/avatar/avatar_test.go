package avatar

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
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
		f.mu.Unlock()
		if redirectTo != "" {
			http.Redirect(w, r, redirectTo, http.StatusFound)
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

// A login the forge does not know is absence, not unreachability: 404, and
// nothing stored. (The question-mark glyph is issue #276's follow-up.)
func TestServeUnknownIdentityIs404AndStoresNothing(t *testing.T) {
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

	rec := get(p, "ghost")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
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
// file is a miss, and says nothing by leaving a partial file behind.
func TestServeFailedTempCreateIsAMissAndStoresNothing(t *testing.T) {
	f := newForgeStub(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("make the store read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	p, err := New(Config{ForgeURL: f.srv.URL, CacheDir: dir})
	if err != nil {
		t.Fatalf("avatar.New: %v", err)
	}

	if code := get(p, "alice").Code; code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when no temp file can be created", code)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("restore the store: %v", err)
	}
	if names := storeFiles(t, dir); len(names) != 0 {
		t.Errorf("store = %v, want nothing written by a failed store", names)
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

// GIF is in the accepted set, so it must actually decode: a registered decoder
// nobody exercises is an entry that cannot fail a test.
func TestServeAcceptsGIFAndStoresPNG(t *testing.T) {
	f := newForgeStub(t)
	var buf bytes.Buffer
	if err := gif.Encode(&buf, makeImage(400, 400), nil); err != nil {
		t.Fatalf("encode gif: %v", err)
	}
	f.mu.Lock()
	f.imageType = "image/gif"
	f.imageBody = buf.Bytes()
	f.mu.Unlock()
	p, dir := newProxy(t, f)

	if code := get(p, "alice").Code; code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a GIF source", code)
	}
	stored, err := os.ReadFile(filepath.Join(dir, "alice.png"))
	if err != nil {
		t.Fatalf("stored render missing: %v", err)
	}
	if _, err := png.Decode(bytes.NewReader(stored)); err != nil {
		t.Errorf("stored render is not a PNG: %v", err)
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

	if first.Code != http.StatusNotFound || second.Code != http.StatusNotFound {
		t.Fatalf("statuses = %d and %d, want 404 both times", first.Code, second.Code)
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
	held := make(chan struct{}, 1)
	var holdMu sync.Mutex
	var holding bool
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
			holdMu.Lock()
			first := !holding
			holding = true
			holdMu.Unlock()
			if first {
				held <- struct{}{} // the first round trip is under way
				<-r.Context().Done()
			}
		}
		orig.ServeHTTP(w, r)
	})
	p, dir := newProxy(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathPrefix+"alice", nil).WithContext(ctx))
		result <- rec
	}()
	<-held
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

	if code := get(p, "alice").Code; code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
	expireMisses(p)
	known.Store(true)
	if code := get(p, "alice").Code; code != http.StatusOK {
		t.Fatalf("status after the window = %d, want 200", code)
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

// JPEG sources are accepted and re-encoded to the canonical PNG render.
func TestServeAcceptsJPEGAndStoresPNG(t *testing.T) {
	f := newForgeStub(t)
	f.mu.Lock()
	f.imageType = "image/jpeg"
	f.imageBody = encodeJPEG(t, 400, 400)
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
}
