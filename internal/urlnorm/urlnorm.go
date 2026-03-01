// Package urlnorm provides aggressive URL normalization for dedup.
// Two URLs that would return the same content should normalize to the
// same string, reducing redundant fetches.
package urlnorm

import (
	"net/url"
	"sort"
	"strings"
)

// Tracking query parameters to strip. These carry no semantic content
// for the page and cause massive dedup misses.
var trackingParams = map[string]struct{}{
	// Google Analytics / Ads
	"utm_source": {}, "utm_medium": {}, "utm_campaign": {},
	"utm_term": {}, "utm_content": {}, "utm_id": {},
	"gclid": {}, "gclsrc": {}, "dclid": {},
	// Facebook
	"fbclid": {}, "fb_action_ids": {}, "fb_action_types": {},
	"fb_source": {}, "fb_ref": {},
	// Microsoft / Bing
	"msclkid": {},
	// HubSpot
	"_hsenc": {}, "_hsmi": {}, "hsa_cam": {}, "hsa_grp": {},
	"hsa_mt": {}, "hsa_src": {}, "hsa_ad": {}, "hsa_acc": {},
	"hsa_net": {}, "hsa_ver": {}, "hsa_la": {}, "hsa_ol": {},
	"hsa_kw": {}, "hsa_tgt": {},
	// Mailchimp
	"mc_cid": {}, "mc_eid": {},
	// Generic
	"ref": {}, "referrer": {},
}

// Normalize applies aggressive normalization to a raw URL string.
// Returns the normalized URL or empty string if the URL is invalid.
func Normalize(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	// Only http/https
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}

	// Lowercase scheme + host
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)

	// Strip default ports
	host := u.Hostname()
	port := u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		u.Host = host
	}

	// Strip www. prefix
	if strings.HasPrefix(u.Host, "www.") {
		u.Host = u.Host[4:]
	}

	// Normalize path: decode unnecessary percent-encoding, remove trailing slash
	p := u.Path
	if p == "" {
		p = "/"
	}
	// Remove trailing slash unless it's the root
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimRight(p, "/")
	}
	// Collapse double slashes
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	u.Path = p

	// Strip fragment
	u.Fragment = ""
	u.RawFragment = ""

	// Process query: remove tracking params, sort remaining
	if u.RawQuery != "" {
		q := u.Query()
		cleaned := make(url.Values)
		for key, vals := range q {
			if _, isTracking := trackingParams[strings.ToLower(key)]; isTracking {
				continue
			}
			// Strip empty-value params that are likely tracking
			if len(vals) == 1 && vals[0] == "" {
				continue
			}
			cleaned[key] = vals
		}
		u.RawQuery = sortedQuery(cleaned)
	}

	return u.String()
}

// sortedQuery encodes url.Values with keys sorted alphabetically.
// This ensures the same query params in different order normalize identically.
func sortedQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}

	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		vals := v[k]
		sort.Strings(vals) // also sort values for determinism
		for j, val := range vals {
			if i > 0 || j > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(val))
		}
	}
	return b.String()
}

// NormalizeDomain extracts and normalizes just the domain from a URL.
// Strips www. prefix and lowercases.
func NormalizeDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if strings.HasPrefix(host, "www.") {
		host = host[4:]
	}
	return host
}
