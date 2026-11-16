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
package avatar

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"image"
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
// the identity and is not remembered (see Proxy.settle). Cache policy in full
// is issue #276's; this is only the failure discipline of this route.
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

	if data, err := os.ReadFile(p.entry(login)); err == nil {
		writeTile(w, data)
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
		writeTile(w, data)
	case http.StatusNotFound:
		// Absence, not unreachability. (The question-mark glyph is
		// issue #276's follow-up.)
		http.Error(w, "identity not found", http.StatusNotFound)
	default:
		// A miss is a miss: the caller's own short line, never the
		// upstream body. #276 layers the cache policy (glyphs, stale
		// serving) on top.
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
	}
}

func writeTile(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	_, _ = w.Write(data)
}

// resolve answers a login from the store, from the memory of a recent miss,
// from the fetch another viewer of the same login already has in progress, or
// by starting that fetch. It is the only path to the forge, and it keeps "one
// identity, one round trip" per login rather than one for the whole server:
// viewers of the same identity share one fetch, viewers of different ones
// never wait on each other.
func (p *Proxy) resolve(ctx context.Context, login string) ([]byte, int, error) {
	for {
		// One short pass under the lock: the store, the memory of a recent
		// miss, then whether this login already has a fetch running. Re-checking
		// the store is what keeps a queued caller from paying for a round trip
		// the fetch it waited on has already made.
		p.mu.Lock()
		if data, err := os.ReadFile(p.entry(login)); err == nil {
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

		data, status, err := p.fetch(ctx, login)
		p.settle(ctx, login, f, data, status, err)
		return data, status, err
	}
}

// settle publishes a fetch's result, records an answered miss, and takes the
// login's flight off the map so the next viewer starts from the store. ctx is
// the context of the request that led the fetch, and it is what a miss is read
// against: a fetch whose own request was still live asked the forge and got
// nothing back, so the miss says something about the identity and is remembered
// for negativeTTL — the client's 15s timeout included, since it is the forge
// that failed to answer and a degraded upstream is exactly what the memory
// exists to stop this route re-probing once per inbound request. A fetch ended
// by that request's context — a browser that dropped an in-flight <img>, a
// Server.Shutdown, a viewer-side deadline — has no answer to hand on, so it
// keeps its 502 to itself rather than writing a minute of empty faces over a
// healthy login.
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
// stores anything. Callers reach it through resolve, which lets one login's
// fetch run at a time.
func (p *Proxy) fetch(ctx context.Context, login string) ([]byte, int, error) {
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
	imgResp, err := p.client.Do(imgReq)
	if err != nil {
		return fail(http.StatusBadGateway, "GET %s: %w", imgURL, err)
	}
	defer imgResp.Body.Close()
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
	if err := p.store(login, render); err != nil {
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

// store writes the render temp-and-rename, so a reader either sees the old
// entry or the new one, never a partial file. A failed rename is a miss, not
// corruption: the temp file is removed and the error returned.
func (p *Proxy) store(login string, render []byte) error {
	tmp, err := os.CreateTemp(p.cacheDir, login+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp render: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(render); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp render: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp render: %w", err)
	}
	if err := os.Rename(tmpName, p.entry(login)); err != nil {
		return fmt.Errorf("rename render into place: %w", err)
	}
	return nil
}
