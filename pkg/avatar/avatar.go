// Package avatar implements hotelier's avatar proxy: it resolves a forge
// login to a small normalised PNG tile, because the forge is mTLS-only at
// the ingress and no browser may fetch it directly (issue #275).
//
// The shape is exactly two upstream requests per identity: GET
// /api/v1/users/<login> on the forge to read avatar_url, then one GET of
// that URL. Redirects are never chased and avatar_url is only followed when
// it stays on the forge origin (site invariant). The forge ignores size
// parameters and returns source-dimension images, so the proxy resizes once
// and stores the small render, keyed by login: the email-hash in the avatar
// URL is never a cache key, never a stored key, never the identity.
//
// Failure discipline: status and content type are both validated before
// anything is written. A non-image or an error body is a miss, never a
// body. The render is written temp-and-rename, so a failed rename leaves
// the previous entry intact rather than a half-written file, and a miss is
// remembered briefly so the route cannot be made to re-pay the forge once
// per inbound request.
//
// Cache policy (issue #276): every tile the proxy serves carries
// Cache-Control: max-age=86400, stale-while-revalidate=604800,
// stale-if-error=2592000 — one day fresh, a week servable with background
// refresh, a month on error — and must-revalidate appears nowhere hotelier
// emits. The store obeys the windows it advertises: a render serves for at
// most max-age plus stale-if-error, and an answer served past max-age carries
// Age so no downstream cache reads an old face as freshly minted. Past that
// outer window the request goes the forge's way rather than the file's, since
// RFC 9110 §4.2.4 gives a cache no licence for bytes staler than the policy
// allows. The proxy mints its own strong content-hashed ETag because it must
// (RFC 9110 §8.3.2 forbids reusing a strong validator across differing
// representations; the proxy serves a re-encoded PNG where the forge served
// the source image): no upstream validator is ever forwarded, and the
// upstream Last-Modified is carried only as If-Modified-Since on the proxy's
// own revalidation requests. Absence (an identity the forge does not know)
// is a cached 200 with a question-mark glyph; unreachability is a 502 with
// no substitution body, so stale-if-error keeps the face a viewer already
// has rather than painting an outage over a good cached tile.
package avatar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"  // registered so image.Decode can read GIF sources
	_ "image/jpeg" // registered so image.Decode can read JPEG sources
	"image/png"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/image/draw"
)

// DefaultTileSize is the edge length, in pixels, of the canonical render.
// One canonical size per identity; CSS scales the tile from here.
const DefaultTileSize = 64

// PathPrefix is the route the proxy is mounted on.
const PathPrefix = "/avatar/"

// The proxy's cache policy (issue #276): one day fresh, a week servable with
// background refresh, a month on error, and must-revalidate nowhere hotelier
// emits. stale-if-error is what makes a forge outage invisible: a downstream
// cache keeps the face it already has, which is why the 502 path substitutes
// no body of its own.
const (
	maxAgeSeconds               = 86400
	staleWhileRevalidateSeconds = 604800
	staleIfErrorSeconds         = 2592000
)

// maxAge is the freshness window the store itself obeys: past it a render is
// served at once and revalidated in the background (the server's own half of
// stale-while-revalidate).
const maxAge = maxAgeSeconds * time.Second

// maxStaleAge is the oldest a stored render may be and still be served: the
// freshness window plus the widest staleness the policy advertises. The
// stale-if-error window is the larger of the two staleness windows, and it
// subsumes stale-while-revalidate here because every stale answer the store
// gives is served alongside a revalidation attempt, rate-bounded by the memory
// of recent misses (see Proxy.revalidate). RFC 9110 §4.2.4 lets a cache use a
// stale response while revalidating and on error, and no further: past this
// age the store has no licence for its own bytes, so the request takes the
// forge path — a fresh render, or a bodyless 502 that lets the caches
// downstream apply stale-if-error themselves.
const maxStaleAge = (maxAgeSeconds + staleIfErrorSeconds) * time.Second

var cacheControlValue = fmt.Sprintf("max-age=%d, stale-while-revalidate=%d, stale-if-error=%d",
	maxAgeSeconds, staleWhileRevalidateSeconds, staleIfErrorSeconds)

// loginPattern admits exactly the characters a forge login may carry that
// are also safe as a filename and a single path segment. It can never
// contain a slash, ".." as a whole name, or anything needing escaping: the
// login doubles as the cache key and the store's file name.
var loginPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,39}$`)

// acceptedImageTypes are the content types the proxy will decode. Anything
// else — the ingress's HTML error pages included — is a miss. The render is
// always re-encoded as PNG, so the accepted set describes what may come in,
// not what goes out.
var acceptedImageTypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
}

// Config configures the proxy.
type Config struct {
	// ForgeURL is the forge's absolute https (or test http) origin, e.g.
	// "https://forge.example.com".
	ForgeURL string
	// CacheDir is the directory holding one PNG per login.
	CacheDir string
	// TLSClientConfig is the transport configuration for the forge hop: nil
	// to present no client certificate, otherwise the pair the forge's
	// ingress demands, built by config.ServerConfig.TLSConfig so the pair
	// has one home (issue #275). A non-nil config carrying a certificate
	// over a plain http forge_url is refused: the credential would be
	// dropped on the floor without a word.
	TLSClientConfig *tls.Config
	// Logf receives one quiet line per miss that reached the forge, naming
	// the login, the upstream and the cause. The response the browser sees
	// is the same short line either way; this is what makes "no faces on the
	// dashboard" answerable from the journal. Nil keeps the proxy silent.
	Logf func(format string, args ...any)
}

// Proxy serves normalised avatar tiles for forge logins.
type Proxy struct {
	forge    *url.URL
	client   *http.Client
	cacheDir string
	// mu guards the two maps below and nothing else: it is never held across
	// a round trip, so one login's slow or hung fetch cannot stall another's
	// for the length of the client's timeout.
	mu sync.Mutex
	// flights holds the fetch each login has in progress, so a cold store
	// asks the forge for one identity once however many dashboards opened at
	// the same time, while different identities are fetched at once. Guarded
	// by mu.
	flights map[string]*flight
	// neg remembers recent misses, keyed by login, so an unauthenticated
	// caller cannot keep the forge's certificate-authenticated path hot one
	// login at a time. Guarded by mu.
	neg map[string]miss
	// logf is never nil; New gives it a sink that discards.
	logf func(format string, args ...any)
}

// flight is one fetch in progress, shared by every viewer of the same login.
// The leader publishes its result and closes done; both happen under mu, so a
// waiter that has observed done also sees the result.
type flight struct {
	done   chan struct{}
	data   []byte
	status int
	err    error
	// abandoned says the fetch was ended by a viewer's own request rather than
	// by the forge, so it carries no verdict: a viewer still queued behind it
	// looks again rather than inheriting a miss nobody was answered.
	abandoned bool
}

// miss is a remembered absence: the status the identity earned, and when the
// memory expires and the forge is asked again.
type miss struct {
	status  int
	expires time.Time
}

// negativeTTL is how long a miss is remembered. Long enough that a dashboard
// render of an identity without a face costs one round trip rather than one
// per request, short enough that a login created or repaired after the miss is
// not shut out for longer than a reload cycle. The window covers misses the
// forge actually gave us: a request the viewer abandoned is not a verdict on
// the identity and is not remembered (see Proxy.settle). An absence answered
// this way is still a cacheable 200 with the glyph (issue #276) — the memory
// stands in for a stored entry, so the store itself stays scoped to identities
// with real renders.
const negativeTTL = 60 * time.Second

// negativeMaxEntries bounds what a stream of invented logins can hold in
// memory.
const negativeMaxEntries = 4096

// New validates the configuration and builds the proxy.
func New(cfg Config) (*Proxy, error) {
	u, err := url.Parse(cfg.ForgeURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("forge_url %q is not an absolute http(s) origin", cfg.ForgeURL)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSClientConfig != nil {
		if u.Scheme != "https" {
			return nil, fmt.Errorf("forge_url %q is not https, so a forge client certificate would never be presented", cfg.ForgeURL)
		}
		transport.TLSClientConfig = cfg.TLSClientConfig.Clone()
	}
	dir := cfg.CacheDir
	if dir == "" {
		return nil, fmt.Errorf("avatar cache dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create avatar cache dir: %w", err)
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	p := &Proxy{
		forge: u,
		client: &http.Client{
			Transport: transport,
			Timeout:   15 * time.Second,
			// Redirects are never chased (issue #275): a 3xx surfaces
			// to the caller of Do as the 3xx response itself, and the
			// fetch treats it as a miss.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		cacheDir: dir,
		flights:  map[string]*flight{},
		neg:      map[string]miss{},
		logf:     logf,
	}
	return p, nil
}

// ServeHTTP serves GET /avatar/<login>.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	login := strings.TrimPrefix(r.URL.Path, PathPrefix)
	if !loginPattern.MatchString(login) {
		http.Error(w, "invalid login", http.StatusBadRequest)
		return
	}

	if data, age, ok := p.stored(login); ok && age <= maxStaleAge {
		writeTile(w, r, data, age)
		// stale-while-revalidate, the server's own half: past max-age the
		// stored render answers as it stands and a background revalidation
		// refreshes it for whoever arrives next. A failed refresh changes
		// nothing here — the render on disk is untouched until a better one
		// arrives, which is stale-if-error honoured at home (issue #276).
		if age > maxAge {
			p.revalidate(login)
		}
		return
	}

	data, status, err := p.resolve(r.Context(), login)
	if err != nil {
		// The browser gets the same short line it always got; the journal
		// gets the cause. A miss that was merely remembered says nothing,
		// because it was said when it happened.
		p.logf("avatar miss for %q: %v", login, err)
	}
	switch status {
	case http.StatusOK:
		// Bytes from the forge path are as fresh as this response: fetched
		// now, or read from the entry a fetch wrote moments ago, so there is
		// no age to stamp.
		writeTile(w, r, data, 0)
	case http.StatusNotFound:
		// Absence, not unreachability: the question-mark glyph answered as a
		// 200, cacheable like any other answer, so a deleted or unknown login
		// settles into the same one-day rhythm as a face instead of sending
		// every viewer back through the forge (issue #276).
		writeTile(w, r, questionGlyph(), 0)
	default:
		// A miss is a miss: the caller's own short line with no substitution
		// body, so stale-if-error keeps the face a viewer already has rather
		// than letting an outage overwrite a good cached tile (issue #276).
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
	}
}

// writeTile answers with a tile — a stored render, a freshly fetched one, or
// the glyph standing for absence — under the proxy's cache policy and a
// validator minted from these exact bytes (issue #276). Because the proxy
// forwards no upstream validator, what it mints can never equal one; a
// validator that differs between the proxy's re-encoding and the forge's
// source is exactly what RFC 9110 §8.3.2 demands.
// age is how long the bytes have been this store's entry: zero for a render
// the forge just handed over, the file's mtime age for one read back off disk.
// An answer with any age at all says so with Age (RFC 9110 §5.1), because
// net/http stamps Date to this moment and a reader given no Age would take a
// weeks-old face for one refreshed seconds ago and advertise a fresh max-age of
// its own.
func writeTile(w http.ResponseWriter, r *http.Request, data []byte, age time.Duration) {
	etag := mintETag(data)
	h := w.Header()
	h.Set("Content-Type", "image/png")
	h.Set("Cache-Control", cacheControlValue)
	h.Set("ETag", etag)
	if age > 0 {
		h.Set("Age", strconv.Itoa(int(age.Seconds())))
	}
	if noneMatch(r, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.Set("Content-Length", fmt.Sprint(len(data)))
	_, _ = w.Write(data)
}

// mintETag is the content hash of the served bytes, quoted per RFC 9110 §6.
// Same bytes, same validator, on the glyph as much as on a face.
func mintETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// noneMatch reports whether the request says it already holds these bytes.
// Comparison is weak (a W/-prefixed candidate matches on the opaque value,
// and "*" matches any representation this route has ever served).
func noneMatch(r *http.Request, etag string) bool {
	for _, candidate := range strings.Split(r.Header.Get("If-None-Match"), ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		if strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}

// resolve answers a login from the store, from the memory of a recent miss,
// from the fetch another viewer of the same login already has in progress, or
// by starting that fetch. It is the path a request takes to the forge (a
// background revalidation takes its own route through revalidate), and it
// keeps "one identity, one round trip" per login rather than one for the
// whole server: viewers of the same identity share one fetch, viewers of
// different ones never wait on each other.
func (p *Proxy) resolve(ctx context.Context, login string) ([]byte, int, error) {
	for {
		// One short pass under the lock: the store, the memory of a recent
		// miss, then whether this login already has a fetch running. Re-checking
		// the store is what keeps a queued caller from paying for a round trip
		// the fetch it waited on has already made.
		p.mu.Lock()
		if data, age, ok := p.stored(login); ok && age <= maxStaleAge {
			p.mu.Unlock()
			return data, http.StatusOK, nil
		}
		if m, ok := p.neg[login]; ok && time.Now().Before(m.expires) {
			p.mu.Unlock()
			return nil, m.status, nil
		}
		f, leads := p.flights[login], false
		if f == nil {
			f = &flight{done: make(chan struct{})}
			p.flights[login] = f
			leads = true
		}
		p.mu.Unlock()

		if !leads {
			select {
			case <-f.done:
			case <-ctx.Done():
				// This viewer left. The fetch they were waiting on carries on for
				// whoever stayed, and leaving is no verdict on the login.
				return nil, http.StatusBadGateway, ctx.Err()
			}
			if f.abandoned {
				// The request that led the shared fetch was cancelled before the
				// forge answered, so it has nothing to hand on. Look again: the
				// store if it got that far, otherwise a fetch of this viewer's.
				continue
			}
			return f.data, f.status, f.err
		}

		data, status, err := p.fetch(ctx, login, "")
		p.settle(ctx, login, f, data, status, err)
		return data, status, err
	}
}

// stored reads a login's render and says how long those bytes have been the
// entry. The render file's own mtime is the clock: store writes it
// temp-and-rename, so its mtime is the moment these bytes became the entry. A
// render that cannot be read is no answer at all (ok is false); one replaced
// between the stat and the read answers with the older age it looked to have,
// which only asks the forge sooner.
//
// This clock has one direction it can lie in: a file cannot be older than the
// store, but the wall clock can be behind it — an NTP correction stepping the
// clock back after boot, a store copied off a host that was running fast. A
// negative age would clear every window below at once, so an age below zero is
// read as just past max-age: the render answers as it always has, and the
// stale path runs behind it rather than never running.
func (p *Proxy) stored(login string) (data []byte, age time.Duration, ok bool) {
	path := p.entry(login)
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, false
	}
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, 0, false
	}
	age = time.Since(info.ModTime())
	if age < 0 {
		age = maxAge + time.Second
	}
	return data, age, true
}

// revalidate starts the background refresh a stale entry deserves, deduped
// through the same per-login flights map that keeps foreground fetches to one
// round trip: a refresh already running for this login is the only refresh
// it needs. The fetch asks the forge whether the image the entry was made
// from has changed, carrying the upstream Last-Modified saved when the render
// was stored — the one place a site's validator is ever forwarded (issue
// #276). Nobody waits on this call: the caller has already been answered with
// the stored render, and an outage here changes nothing on disk.
func (p *Proxy) revalidate(login string) {
	p.mu.Lock()
	// The memory of a recent miss gates this path as it gates resolve. A
	// remembered failure is no verdict on this entry's freshness, but it is a
	// reason not to pay for another round trip yet: without it an entry past
	// max-age turns one inbound request into one forge hop for as long as the
	// forge keeps failing fast — the amplification negativeTTL exists to bound,
	// since the flights map only deduplicates fetches that are in progress
	// (issue #276).
	if m, remembered := p.neg[login]; remembered && time.Now().Before(m.expires) {
		p.mu.Unlock()
		return
	}
	if _, busy := p.flights[login]; busy {
		p.mu.Unlock()
		return
	}
	f := &flight{done: make(chan struct{})}
	p.flights[login] = f
	p.mu.Unlock()

	go func() {
		// No request context outlives its response; this fetch answers to the
		// client's own timeout instead.
		ctx := context.Background()
		data, status, err := p.fetch(ctx, login, p.ifModifiedSince(login))
		p.settle(ctx, login, f, data, status, err)
		if err != nil {
			p.logf("avatar revalidation miss for %q: %v", login, err)
		}
	}()
}

// settle publishes a fetch's result, records an answered miss, and takes the
// login's flight off the map so the next viewer starts from the store. A miss
// is read against ctx — the context of the request that led the fetch: a miss
// with that context still live is the forge failing to answer and is
// remembered for negativeTTL; a fetch ended by that context has no answer to
// hand on and keeps its 502 to itself.
func (p *Proxy) settle(ctx context.Context, login string, f *flight, data []byte, status int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.flights, login)
	if status == http.StatusOK {
		delete(p.neg, login)
	} else {
		f.abandoned = ctx.Err() != nil
		if !f.abandoned {
			p.rememberMiss(login, status)
		}
	}
	f.data, f.status, f.err = data, status, err
	close(f.done)
}

// rememberMiss records the status a miss earned, keeping the memory bounded by
// recycling expired entries first and dropping it whole when none have
// expired.
func (p *Proxy) rememberMiss(login string, status int) {
	now := time.Now()
	if len(p.neg) >= negativeMaxEntries {
		for name, m := range p.neg {
			if !now.Before(m.expires) {
				delete(p.neg, name)
			}
		}
		if len(p.neg) >= negativeMaxEntries {
			clear(p.neg)
		}
	}
	p.neg[login] = miss{status: status, expires: now.Add(negativeTTL)}
}

// fetch performs the two-request round trip, stores the render, and returns
// the status this request deserves: 200 with the tile, 404 for an identity
// the forge does not know, or 502 for every other miss, and the cause of a
// miss for the log — the response body never carries it. No failure path
// stores anything. Callers reach it through resolve (cold, ifModifiedSince
// empty) or revalidate (carrying the stored upstream Last-Modified so the
// image hop is conditional, issue #276); either way one login's fetch runs at
// a time.
func (p *Proxy) fetch(ctx context.Context, login, ifModifiedSince string) ([]byte, int, error) {
	// fail names the cause behind a miss. Every return below is a different
	// story in the journal — an expired client certificate, an ingress that
	// answered 400 with HTML, a cache directory that went read-only — even
	// though the browser sees one short line for all of them.
	fail := func(status int, format string, args ...any) ([]byte, int, error) {
		return nil, status, fmt.Errorf(format, args...)
	}

	// Request 1: the identity's avatar_url from the forge API. A 404 is
	// absence, not unreachability; say so distinctly. Everything else
	// non-200 (the ingress's 4xx HTML pages included) is a miss.
	apiURL := p.forge.JoinPath("/api/v1/users/", login).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return fail(http.StatusBadGateway, "building GET %s: %w", apiURL, err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fail(http.StatusBadGateway, "GET %s: %w", apiURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		// Absence is a verdict: forget any render this login left behind, so
		// a deleted account converges on the glyph instead of serving an old
		// face through stale-if-error forever. Errors keep the face (a later
		// revalidation gets another go); absence drops it (issue #276).
		p.forget(login)
		return fail(http.StatusNotFound, "GET %s: no such identity", apiURL)
	}
	if resp.StatusCode != http.StatusOK {
		return fail(http.StatusBadGateway, "GET %s: upstream status %d", apiURL, resp.StatusCode)
	}
	var user struct {
		AvatarURL string `json:"avatar_url"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&user); err != nil {
		return fail(http.StatusBadGateway, "decoding GET %s: %w", apiURL, err)
	}
	if user.AvatarURL == "" {
		return fail(http.StatusBadGateway, "GET %s names no avatar_url", apiURL)
	}

	// avatar_url never points off the forge (site invariant). Enforce it:
	// an off-origin URL is a miss and is never requested.
	imgURL, err := p.forge.Parse(user.AvatarURL)
	if err != nil || imgURL.Scheme != p.forge.Scheme || imgURL.Host != p.forge.Host {
		return fail(http.StatusBadGateway, "avatar_url %q is not on the forge origin %s", user.AvatarURL, p.forge.Host)
	}

	// Request 2: the image itself. Redirects are not chased (the client is
	// configured to stop at the first response), so a 3xx lands here as a
	// non-200 miss.
	imgReq, err := http.NewRequestWithContext(ctx, http.MethodGet, imgURL.String(), nil)
	if err != nil {
		return fail(http.StatusBadGateway, "building GET %s: %w", imgURL, err)
	}
	if ifModifiedSince != "" {
		imgReq.Header.Set("If-Modified-Since", ifModifiedSince)
	}
	imgResp, err := p.client.Do(imgReq)
	if err != nil {
		return fail(http.StatusBadGateway, "GET %s: %w", imgURL, err)
	}
	defer imgResp.Body.Close()
	if imgResp.StatusCode == http.StatusNotModified {
		// The render on disk is current: wind its clock and hand it back.
		// Nothing is rewritten — a revalidation that changes nothing must not
		// spin the store.
		if err := os.Chtimes(p.entry(login), time.Now(), time.Now()); err != nil {
			return fail(http.StatusBadGateway, "winding the entry for %q: %w", login, err)
		}
		data, err := os.ReadFile(p.entry(login))
		if err != nil {
			return fail(http.StatusBadGateway, "rereading the entry for %q: %w", login, err)
		}
		return data, http.StatusOK, nil
	}
	if imgResp.StatusCode != http.StatusOK {
		return fail(http.StatusBadGateway, "GET %s: upstream status %d", imgURL, imgResp.StatusCode)
	}
	// Validate the content type before anything is written: the ingress
	// answers some failures 200-shaped with HTML, and a non-image is a
	// miss, never a body.
	mediaType, _, err := mime.ParseMediaType(imgResp.Header.Get("Content-Type"))
	if err != nil || !acceptedImageTypes[mediaType] {
		return fail(http.StatusBadGateway, "GET %s: content type %q is not an image", imgURL, imgResp.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(io.LimitReader(imgResp.Body, maxResponseBytes))
	if err != nil {
		return fail(http.StatusBadGateway, "reading GET %s: %w", imgURL, err)
	}
	render, err := normalise(body)
	if err != nil {
		return fail(http.StatusBadGateway, "GET %s: %w", imgURL, err)
	}
	if err := p.store(login, render, imgResp.Header.Get("Last-Modified")); err != nil {
		return fail(http.StatusBadGateway, "storing the render for %q: %w", login, err)
	}
	return render, http.StatusOK, nil
}

// maxResponseBytes bounds what one upstream response may deliver; avatars
// are kilobytes, and anything approaching this is not an avatar.
const maxResponseBytes = 8 << 20

// maxSourcePixels bounds the *decoded* frame, which the byte cap above cannot:
// a 646 KB PNG can declare 10000x10000 and ask the process for 381 MB. The
// ceiling sits comfortably above the 592² source dimensions the forge returns
// (issue #275), so it refuses a hostile header rather than a real avatar.
const maxSourcePixels = 4096 * 4096

// errTooLarge marks a source whose own header declares more pixels than the
// ceiling allows. It is a miss like any other: nothing is stored.
var errTooLarge = errors.New("avatar declares too many pixels")

// normalise decodes the source image, once its header's declared dimensions
// have cleared the pixel ceiling, and produces the single canonical render:
// the longest edge fitted into DefaultTileSize x DefaultTileSize, centred on a
// transparent square, re-encoded as PNG. Generated identicons pass through
// as ordinary avatars (decision recorded on the tracker): the API exposes
// no "has explicit avatar" signal, so suppression would be guesswork.
func normalise(body []byte) ([]byte, error) {
	// Read the header before decoding: the dimensions a file claims are what
	// the decoder would allocate, and honouring an unchecked claim would let
	// a few hundred bytes cost hundreds of megabytes.
	cfg, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("read avatar image header: %w", err)
	}
	if cfg.Width <= 0 || cfg.Height <= 0 ||
		int64(cfg.Width)*int64(cfg.Height) > int64(maxSourcePixels) {
		return nil, fmt.Errorf("%w: %dx%d over %d", errTooLarge, cfg.Width, cfg.Height, maxSourcePixels)
	}
	img, _, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("decode avatar image: %w", err)
	}
	b := img.Bounds().Dx()
	if h := img.Bounds().Dy(); h > b {
		b = h
	}
	scale := float64(DefaultTileSize) / float64(b)
	w := max(1, int(float64(img.Bounds().Dx())*scale))
	h := max(1, int(float64(img.Bounds().Dy())*scale))
	dst := image.NewNRGBA(image.Rect(0, 0, DefaultTileSize, DefaultTileSize))
	x := (DefaultTileSize - w) / 2
	y := (DefaultTileSize - h) / 2
	draw.CatmullRom.Scale(dst, image.Rect(x, y, x+w, y+h), img, img.Bounds(), draw.Over, nil)
	var out bytes.Buffer
	if err := png.Encode(&out, dst); err != nil {
		return nil, fmt.Errorf("encode normalised tile: %w", err)
	}
	return out.Bytes(), nil
}

// entry names the file a login's render is stored in. The login is the key the
// whole package is named after, so reads and renames must never derive it
// separately.
func (p *Proxy) entry(login string) string {
	return filepath.Join(p.cacheDir, login+".png")
}

// lastModifiedFile names the sidecar holding the upstream Last-Modified of
// the image a render was made from. It lives beside the render because it is
// part of the render's provenance, and it never leaves the store: the proxy
// mints its own validators and forwards this one only as If-Modified-Since on
// its own conditional requests (issue #276).
func (p *Proxy) lastModifiedFile(login string) string {
	return filepath.Join(p.cacheDir, login+".last-modified")
}

// ifModifiedSince reads the sidecar. Anything unreadable reads as no
// validator, which asks the forge unconditionally — the safe direction for a
// torn write.
func (p *Proxy) ifModifiedSince(login string) string {
	v, err := os.ReadFile(p.lastModifiedFile(login))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(v))
}

// forget removes a login's entry and its sidecar. It is called when the forge
// has said, definitively, that the identity is gone.
func (p *Proxy) forget(login string) {
	for _, path := range []string{p.entry(login), p.lastModifiedFile(login)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			p.logf("avatar: forgetting %s: %v", filepath.Base(path), err)
		}
	}
}

// store writes the render temp-and-rename, so a reader either sees the old
// entry or the new one, never a partial file, then records what the forge
// said about the image's age beside it. A failed rename of the render is a
// miss, not corruption: the temp file is removed and the error returned, and
// the entry keeps whatever it provably held before. The sidecar is held to a
// lower standard, as forget holds it: it only sharpens the next
// revalidation's If-Modified-Since, so losing it while the render is already
// in place is worth a log line rather than a miss for bytes the next viewer
// will be served anyway.
func (p *Proxy) store(login string, render []byte, lastModified string) error {
	if err := p.writeRenamed(p.entry(login), login+".tmp-*", render); err != nil {
		return err
	}
	if lastModified == "" {
		if err := os.Remove(p.lastModifiedFile(login)); err != nil && !errors.Is(err, os.ErrNotExist) {
			p.logf("avatar: forgetting the upstream last-modified for %q: %v", login, err)
		}
		return nil
	}
	if err := p.writeRenamed(p.lastModifiedFile(login), login+".lm-*", []byte(lastModified)); err != nil {
		p.logf("avatar: storing the upstream last-modified for %q: %v", login, err)
	}
	return nil
}

// writeRenamed puts data at final temp-and-rename, so a reader either sees
// the old file or the new one, never a partial file.
func (p *Proxy) writeRenamed(final, pattern string, data []byte) error {
	tmp, err := os.CreateTemp(p.cacheDir, pattern)
	if err != nil {
		return fmt.Errorf("create temp file beside %s: %w", filepath.Base(final), err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp file %s: %w", filepath.Base(tmpName), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file %s: %w", filepath.Base(tmpName), err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("rename %s into place: %w", filepath.Base(tmpName), err)
	}
	return nil
}

// The absence glyph: a question mark on a disc, drawn from fixed rows so
// every hotelier and every viewer agrees on the same bytes — and therefore
// on the same minted ETag. The answer is generated, not fetched, but it is
// cached like any other (issue #276).
var glyphRows = []string{
	"  ####  ",
	" #    # ",
	" #    # ",
	"      # ",
	"   ##   ",
	"   #    ",
	"        ",
	"   #    ",
}

var (
	glyphOnce sync.Once
	glyphPNG  []byte
)

func questionGlyph() []byte {
	glyphOnce.Do(func() {
		const size = DefaultTileSize
		img := image.NewNRGBA(image.Rect(0, 0, size, size))
		const centre = (size - 1) / 2.0
		const radius = 30.0
		const inset, scale = 8, 6
		for y := range size {
			for x := range size {
				dx, dy := float64(x)-centre, float64(y)-centre
				if dx*dx+dy*dy > radius*radius {
					continue
				}
				pixel := color.NRGBA{R: 0x52, G: 0x57, B: 0x5e, A: 0xff}
				gx, gy := x-inset, y-inset
				if gx >= 0 && gy >= 0 && gx < scale*len(glyphRows[0]) && gy < scale*len(glyphRows) &&
					glyphRows[gy/scale][gx/scale] == '#' {
					pixel = color.NRGBA{R: 0xf4, G: 0xf4, B: 0xf5, A: 0xff}
				}
				img.Set(x, y, pixel)
			}
		}
		var out bytes.Buffer
		if err := png.Encode(&out, img); err != nil {
			// Encoding an in-memory image cannot fail; getting here is a bug,
			// not a runtime condition, and a silent empty glyph would be worse
			// than loud.
			panic("avatar: encoding the absence glyph: " + err.Error())
		}
		glyphPNG = out.Bytes()
	})
	return glyphPNG
}
