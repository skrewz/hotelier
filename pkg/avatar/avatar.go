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
// the previous entry intact rather than a half-written file.
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
	// ClientCert and ClientKey are the paths of the mTLS client pair the
	// forge's ingress demands. Both empty means no client certificate;
	// one without the other is a configuration error.
	ClientCert string
	ClientKey  string
}

// Proxy serves normalised avatar tiles for forge logins.
type Proxy struct {
	forge    *url.URL
	client   *http.Client
	cacheDir string
	// mu serialises upstream fetches so a cold store cannot stampede the
	// forge with one API plus image request per concurrent viewer.
	mu sync.Mutex
}

// New validates the configuration and builds the proxy.
func New(cfg Config) (*Proxy, error) {
	u, err := url.Parse(cfg.ForgeURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("forge_url %q is not an absolute http(s) origin", cfg.ForgeURL)
	}
	if (cfg.ClientCert == "") != (cfg.ClientKey == "") {
		return nil, fmt.Errorf("forge client cert and key must be set together")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.ClientCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load forge client cert/key: %w", err)
		}
		transport.TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	dir := cfg.CacheDir
	if dir == "" {
		return nil, fmt.Errorf("avatar cache dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create avatar cache dir: %w", err)
	}
	return &Proxy{
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
	}, nil
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

	path := filepath.Join(p.cacheDir, login+".png")
	if data, err := os.ReadFile(path); err == nil {
		writeTile(w, data)
		return
	}

	data, status := p.fetch(r.Context(), login)
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

// fetch performs the two-request round trip, stores the render, and returns
// the status this request deserves: 200 with the tile, 404 for an identity
// the forge does not know, or 502 for every other miss. No failure path
// stores anything.
func (p *Proxy) fetch(ctx context.Context, login string) ([]byte, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-check the store under the lock: a caller that queued behind a
	// fetch for the same login must be served from what that fetch stored,
	// not launch its own round trip.
	if data, err := os.ReadFile(filepath.Join(p.cacheDir, login+".png")); err == nil {
		return data, http.StatusOK
	}

	// Request 1: the identity's avatar_url from the forge API. A 404 is
	// absence, not unreachability; say so distinctly. Everything else
	// non-200 (the ingress's 4xx HTML pages included) is a miss.
	apiURL := p.forge.JoinPath("/api/v1/users/", login).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, http.StatusNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, http.StatusBadGateway
	}
	var user struct {
		AvatarURL string `json:"avatar_url"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&user); err != nil || user.AvatarURL == "" {
		return nil, http.StatusBadGateway
	}

	// avatar_url never points off the forge (site invariant). Enforce it:
	// an off-origin URL is a miss and is never requested.
	imgURL, err := p.forge.Parse(user.AvatarURL)
	if err != nil || imgURL.Scheme != p.forge.Scheme || imgURL.Host != p.forge.Host {
		return nil, http.StatusBadGateway
	}

	// Request 2: the image itself. Redirects are not chased (the client is
	// configured to stop at the first response), so a 3xx lands here as a
	// non-200 miss.
	imgReq, err := http.NewRequestWithContext(ctx, http.MethodGet, imgURL.String(), nil)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	imgResp, err := p.client.Do(imgReq)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	defer imgResp.Body.Close()
	if imgResp.StatusCode != http.StatusOK {
		return nil, http.StatusBadGateway
	}
	// Validate the content type before anything is written: the ingress
	// answers some failures 200-shaped with HTML, and a non-image is a
	// miss, never a body.
	mediaType, _, err := mime.ParseMediaType(imgResp.Header.Get("Content-Type"))
	if err != nil || !acceptedImageTypes[mediaType] {
		return nil, http.StatusBadGateway
	}
	body, err := io.ReadAll(io.LimitReader(imgResp.Body, maxResponseBytes))
	if err != nil {
		return nil, http.StatusBadGateway
	}
	render, err := p.normalise(body)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	if err := p.store(login, render); err != nil {
		return nil, http.StatusBadGateway
	}
	return render, http.StatusOK
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
func (p *Proxy) normalise(body []byte) ([]byte, error) {
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
	if err := os.Rename(tmpName, filepath.Join(p.cacheDir, login+".png")); err != nil {
		return fmt.Errorf("rename render into place: %w", err)
	}
	return nil
}
