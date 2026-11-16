package avatar

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
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

func (f *forgeStub) set(what string, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch what {
	case "avatarURL":
		f.avatarURL = value
	case "imageType":
		f.imageType = value
	}
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
	f.set("avatarURL", f.srv.URL+"/avatars/"+strings.Repeat("b", 64)+".png")

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
	f.set("avatarURL", other.URL+"/x.png")
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
	orig := f.srv.Config.Handler
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/users/") {
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
	time.Sleep(100 * time.Millisecond) // let every goroutine reach ServeHTTP
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
