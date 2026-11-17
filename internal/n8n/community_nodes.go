package n8n

import (
	"encoding/xml"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var communityPlaceholderRe = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// placeholders returns the {name} placeholders in a node's string parameters.
func placeholders(p map[string]interface{}) []string {
	seen := map[string]bool{}
	var walk func(v interface{})
	walk = func(v interface{}) {
		switch t := v.(type) {
		case string:
			for _, m := range communityPlaceholderRe.FindAllStringSubmatch(t, -1) {
				seen[m[1]] = true
			}
		case map[string]interface{}:
			for k, x := range t {
				if k != "toolDescription" && k != "description" {
					walk(x)
				}
			}
		case []interface{}:
			for _, x := range t {
				walk(x)
			}
		}
	}
	walk(p)
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// withPlaceholders returns s with {name} placeholders replaced by args.
func withPlaceholders(s subNode, args map[string]interface{}) subNode {
	var sub func(v interface{}) interface{}
	sub = func(v interface{}) interface{} {
		switch t := v.(type) {
		case string:
			return communityPlaceholderRe.ReplaceAllStringFunc(t, func(m string) string {
				if a, ok := args[m[1:len(m)-1]]; ok {
					s := toString(a)
					if strings.HasPrefix(strings.TrimSpace(t), "={") || strings.HasPrefix(strings.TrimSpace(t), "{") {
						b := mustJSON(s) // inside JSON text: keep it a valid string body
						return b[1 : len(b)-1]
					}
					return s
				}
				return m
			})
		case map[string]interface{}:
			out := make(map[string]interface{}, len(t))
			for k, x := range t {
				out[k] = sub(x)
			}
			return out
		case []interface{}:
			out := make([]interface{}, len(t))
			for i, x := range t {
				out[i] = sub(x)
			}
			return out
		}
		return v
	}
	cfg := make(map[string]interface{}, len(s.config))
	for k, v := range s.config {
		cfg[k] = v
	}
	cfg[cfgParams] = sub(s.params())
	return subNode{typ: s.typ, config: cfg}
}

// ---------------------------------------------------------------- Tavily

func tavilyNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("tavilyApi")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("tavily", "https://api.tavily.com")
	h := map[string]string{"Authorization": "Bearer " + credString(c, "apiKey")}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "search", map[string]string{"search": "search", "extract": "extract"})
		body := map[string]interface{}{}
		for k, v := range fieldsOf(ec, i, "options") {
			body[k] = v
		}
		path := "/search"
		if r == "extract" || o == "extract" {
			path = "/extract"
			u, _ := ec.param(i, "urls", nil)
			urls := asList(u)
			if urls == nil {
				for _, s := range splitList(toString(u)) {
					urls = append(urls, s)
				}
			}
			body["urls"] = urls
		} else {
			q, err := ec.str(i, "query", "")
			if err != nil {
				return nil, err
			}
			if m, ok := parseJSONParam(strings.TrimSpace(q)).(map[string]interface{}); ok {
				for k, v := range m {
					if v != nil {
						body[k] = v
					}
				}
			} else {
				body["query"] = q
			}
		}
		res, _, err := ec.apiCall("POST", base+path, h, body, "")
		return one(res), err
	})
}

// ---------------------------------------------------------------- ScrapeNinja

func scrapeNinjaNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("scrapeNinjaApi")
	if err != nil {
		return nil, err
	}
	base, h := "https://scrapeninja.p.rapidapi.com", map[string]string{"X-RapidAPI-Key": credString(c, "apiKey"), "X-RapidAPI-Host": "scrapeninja.p.rapidapi.com"}
	if credString(c, "marketplace") == "apiroad" {
		base, h = "https://scrapeninja.apiroad.net", map[string]string{"X-Apiroad-Key": credString(c, "apiKey")}
	}
	base = ec.apiBase("scrapeninja", base)
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "scrape")
		u, _ := ec.str(i, "url", "")
		body := map[string]interface{}{"url": u}
		for k, v := range fieldsOf(ec, i, "options") {
			body[k] = v
		}
		path := "/scrape"
		if op == "scrapeJs" {
			path = "/scrape-js"
		}
		res, _, err := ec.apiCall("POST", base+path, h, body, "")
		return one(res), err
	})
}

// ---------------------------------------------------------------- InfraNodus

func infranodusNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("infranodusApi")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("infranodus", "https://infranodus.com") + "/api/v1"
	h := map[string]string{"Authorization": "Bearer " + credString(c, "apiKey")}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		name, _ := ec.str(i, "name", "")
		prompt, err := ec.str(i, "prompt", "")
		if err != nil {
			return nil, err
		}
		q := url.Values{"doNotSave": {"true"}, "addStats": {"true"}, "includeGraph": {"false"}}
		res, _, err := ec.apiCall("POST", base+"/graphAndAdvice?"+q.Encode(), h, map[string]interface{}{"name": name, "requestMode": "response", "prompt": prompt, "aiTopics": "true"}, "")
		if err != nil {
			return nil, err
		}
		if adv := asList(res["aiAdvice"]); len(adv) > 0 {
			res["response"] = asMap(adv[0])["text"]
		}
		return one(res), nil
	})
}

// ---------------------------------------------------------------- Blotato

func blotatoNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("blotatoApi")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("blotato", "https://backend.blotato.com") + "/v2"
	h := map[string]string{"blotato-api-key": credString(c, "blotatoApiKey")}
	if h["blotato-api-key"] == "" {
		h["blotato-api-key"] = credString(c, "apiKey")
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, _ := ec.str(i, "resource", "post")
		if r == "media" {
			u, _ := ec.str(i, "mediaUrl", "")
			res, _, err := ec.apiCall("POST", base+"/media", h, map[string]interface{}{"url": u}, "")
			return one(res), err
		}
		platform, _ := ec.str(i, "platform", "twitter")
		account, _ := ec.rlString(i, "accountId")
		text, _ := ec.str(i, "postContentText", "")
		var media []interface{}
		if mv, _ := ec.param(i, "postContentMediaUrls", ""); mv != nil {
			if l := asList(mv); l != nil {
				media = l
			} else {
				for _, s := range splitList(toString(mv)) {
					media = append(media, s)
				}
			}
		}
		if media == nil {
			media = []interface{}{}
		}
		target := map[string]interface{}{"targetType": platform}
		prefix := "postCreate" + strings.ToUpper(platform[:1]) + platform[1:] + "Option"
		for k := range ec.params {
			if strings.HasPrefix(k, prefix) {
				v, _ := ec.param(i, k, nil)
				opt := strings.TrimPrefix(k, prefix)
				target[strings.ToLower(opt[:1])+opt[1:]] = v
			}
		}
		switch platform {
		case "facebook":
			target["pageId"], _ = ec.rlString(i, "facebookPageId")
		case "pinterest":
			target["boardId"], _ = ec.rlString(i, "pinterestBoardId")
		case "linkedin":
			if p, _ := ec.rlString(i, "linkedinPageId"); p != "" {
				target["pageId"] = p
			}
		case "tiktok":
			for k, v := range map[string]interface{}{"privacyLevel": "PUBLIC_TO_EVERYONE", "disabledComments": false, "disabledDuet": false, "disabledStitch": false, "isBrandedContent": false, "isYourBrand": false, "isAiGenerated": true} {
				if _, ok := target[k]; !ok {
					target[k] = v
				}
			}
		}
		post := map[string]interface{}{"accountId": account, "content": map[string]interface{}{"text": text, "mediaUrls": media, "platform": platform}, "target": target}
		res, _, err := ec.apiCall("POST", base+"/posts", h, map[string]interface{}{"post": post}, "")
		return one(res), err
	})
}

// ---------------------------------------------------------------- YouTube transcripts

type transcriptSegment struct {
	Text     string  `json:"text"`
	Offset   float64 `json:"offset"`
	Duration float64 `json:"duration"`
	Lang     string  `json:"lang,omitempty"`
}

var ytIDRe = regexp.MustCompile(`(?:v=|youtu\.be/|/shorts/|/embed/|/live/)([A-Za-z0-9_-]{11})`)

// fetchYouTubeTranscript reads a video's captions through YouTube's
// innertube player API (preferring lang, else the first track).
func (ec *execCtx) fetchYouTubeTranscript(video, lang string) ([]transcriptSegment, error) {
	if m := ytIDRe.FindStringSubmatch(video); m != nil {
		video = m[1]
	}
	base := ec.apiBase("youtubeweb", "https://www.youtube.com")
	body := map[string]interface{}{"videoId": video, "context": map[string]interface{}{"client": map[string]interface{}{"clientName": "ANDROID", "clientVersion": "20.10.38", "hl": "en"}}}
	res, _, err := ec.apiCall("POST", base+"/youtubei/v1/player?prettyPrint=false", map[string]string{"User-Agent": "com.google.android.youtube/20.10.38 (Linux; U; Android 14)"}, body, "")
	if err != nil {
		return nil, err
	}
	tracks := asList(asMap(asMap(res["captions"])["playerCaptionsTracklistRenderer"])["captionTracks"])
	if len(tracks) == 0 {
		return nil, permanentError{fmt.Sprintf("no transcript is available for video %s", video)}
	}
	track := asMap(tracks[0])
	for _, t := range tracks {
		if lang != "" && strings.HasPrefix(toString(asMap(t)["languageCode"]), lang) {
			track = asMap(t)
			break
		}
	}
	u := regexp.MustCompile(`&fmt=[^&]*`).ReplaceAllString(toString(track["baseUrl"]), "")
	if strings.HasPrefix(u, "/") {
		u = base + u
	}
	data, _, err := ec.downloadWith(u, nil)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Texts []struct {
			Start string `xml:"start,attr"`
			Dur   string `xml:"dur,attr"`
			Body  string `xml:",chardata"`
		} `xml:"text"`
		Ps []struct {
			T    string `xml:"t,attr"`
			D    string `xml:"d,attr"`
			Body string `xml:",innerxml"`
		} `xml:"body>p"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, permanentError{"invalid transcript data"}
	}
	code := toString(track["languageCode"])
	var out []transcriptSegment
	for _, t := range doc.Texts {
		s, _ := strconv.ParseFloat(t.Start, 64)
		d, _ := strconv.ParseFloat(t.Dur, 64)
		out = append(out, transcriptSegment{Text: html.UnescapeString(html.UnescapeString(t.Body)), Offset: s, Duration: d, Lang: code})
	}
	for _, p := range doc.Ps {
		s, _ := strconv.ParseFloat(p.T, 64)
		d, _ := strconv.ParseFloat(p.D, 64)
		text := html.UnescapeString(stripTags(p.Body))
		if strings.TrimSpace(text) == "" {
			continue
		}
		out = append(out, transcriptSegment{Text: text, Offset: s / 1000, Duration: d / 1000, Lang: code})
	}
	return out, nil
}

func youtubeTranscripterNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		id, _ := ec.str(i, "youtubeId", "")
		if id == "" {
			id, _ = ec.str(i, "videoId", "")
		}
		for _, k := range []string{"youtubeId", "videoId", "VIDEO_ID", "url", "youtubeUrl"} {
			if id == "" {
				id = toString(it.JSON[k])
			}
		}
		if id == "" {
			return nil, permanentError{"YouTube Transcript: no video ID"}
		}
		lang, _ := ec.str(i, "options.language", "")
		segs, err := ec.fetchYouTubeTranscript(id, lang)
		if err != nil {
			return nil, err
		}
		list := make([]interface{}, len(segs))
		texts := make([]string, len(segs))
		for k, s := range segs {
			list[k] = map[string]interface{}{"text": s.Text, "offset": s.Offset, "duration": s.Duration}
			texts[k] = s.Text
		}
		return one(map[string]interface{}{"youtubeId": id, "transcript": list, "text": strings.Join(texts, " ")}), nil
	})
}
