package n8n

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Service nodes: Mautic, Google Contacts, NASA, Google Business Profile,
// Home Assistant, Pushover, Dropcontact and lemlist.

// offsetPager advances an offset query parameter by the records received.
func offsetPager(param string) restPager {
	return func(p *restPage, _ interface{}, _ http.Header) bool {
		p.query.Set(param, strconv.Itoa(p.got))
		return true
	}
}

// googleAuthDef is googleAuth for nodes whose authentication parameter
// defaults to def ("serviceAccount" or "oAuth2").
func googleAuthDef(ec *execCtx, i int, scope, oauthType, def string) (map[string]string, error) {
	auth, _ := ec.str(i, "authentication", def)
	if auth == "serviceAccount" {
		c, err := ec.credential("googleApi")
		if err != nil {
			return nil, err
		}
		tok, err := ec.serviceAccountToken(c, scope)
		if err != nil {
			return nil, err
		}
		return map[string]string{"Authorization": "Bearer " + tok}, nil
	}
	c, err := ec.credential(oauthType)
	if err != nil {
		return nil, err
	}
	tok := accessToken(c)
	if tok == "" {
		return nil, permanentError{oauthType + " credential has no access token"}
	}
	return map[string]string{"Authorization": "Bearer " + tok}, nil
}

// kvList turns n8n fixedCollection rows [{name, value}] into an object.
func rowsToMap(rows interface{}, k, v string) map[string]interface{} {
	out := map[string]interface{}{}
	for _, r := range asList(rows) {
		m := asMap(r)
		if key := toString(m[k]); key != "" {
			out[key] = m[v]
		}
	}
	return out
}

func init() {
	registerMautic()
	registerGoogleContacts()
	registerNASA()
	registerGoogleBusinessProfile()
	registerHomeAssistant()
	registerPushover()
	registerDropcontact()
	registerLemlist()
}

// ---------------------------------------------------------------- Mautic

func registerMautic() {
	contactFields := "email firstName:firstname lastName:lastname company position title @additionalFields *lower"
	registerREST("mautic", &restService{
		defResource: "contact",
		defOps:      map[string]string{"contact": "create", "company": "create", "contactSegment": "add", "campaignContact": "add", "segmentEmail": "send", "companyContact": "add"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, t, err := ec.restCred(i, "mauticApi", map[string]string{"credentials": "mauticApi", "oAuth2": "mauticOAuth2Api"})
			if err != nil {
				return nil, err
			}
			base := ec.apiBase("mautic", strings.TrimRight(credString(c, "url"), "/")) + "/api"
			if t == "mauticOAuth2Api" {
				return &restClient{base: base, headers: map[string]string{"Authorization": "Bearer " + accessToken(c)}}, nil
			}
			return &restClient{base: base, headers: map[string]string{"Authorization": basicHeader(credString(c, "username", "user"), credString(c, "password"))}}, nil
		},
		ops: map[string]*restOp{
			"contact/create":               {method: "POST", path: "/contacts/new", body: contactFields, build: mauticJSONBody, out: "contact", post: mauticSimplify},
			"contact/update":               {method: "PATCH", path: "/contacts/{contactId}/edit", body: "@updateFields *lower", build: mauticJSONBody, out: "contact", post: mauticSimplify},
			"contact/get":                  {path: "/contacts/{contactId}", out: "contact", post: mauticSimplify},
			"contact/delete":               {method: "DELETE", path: "/contacts/{contactId}/delete", out: "contact", post: mauticSimplify},
			"contact/getAll":               {run: mauticList("/contacts", "contacts")},
			"contact/editDoNotContactList": {method: "POST", path: "/contacts/{contactId}/dnc/{channel}/{action}", body: "@additionalFields reason comments", out: "contact", post: mauticSimplify},
			"contact/editContactPoint": {method: "POST", path: "/contacts/{contactId}/points/{action}/{value}", success: true, build: func(ec *execCtx, i int, r *restRequest) error {
				a, _ := ec.str(i, "action", "add")
				r.path = strings.Replace(r.path, "/points/"+a+"/", map[string]string{"add": "/points/plus/", "remove": "/points/minus/"}[a], 1)
				return nil
			}},
			"contact/sendEmail":      {method: "POST", path: "/emails/{campaignEmailId}/contact/{contactId}/send", success: true},
			"contactSegment/add":     {method: "POST", path: "/segments/{segmentId}/contact/{contactId}/add", success: true},
			"contactSegment/remove":  {method: "POST", path: "/segments/{segmentId}/contact/{contactId}/remove", success: true},
			"campaignContact/add":    {method: "POST", path: "/campaigns/{campaignId}/contact/{contactId}/add", success: true},
			"campaignContact/remove": {method: "POST", path: "/campaigns/{campaignId}/contact/{contactId}/remove", success: true},
			"companyContact/add":     {method: "POST", path: "/companies/{companyId}/contact/{contactId}/add", success: true},
			"companyContact/remove":  {method: "POST", path: "/companies/{companyId}/contact/{contactId}/remove", success: true},
			"segmentEmail/send":      {method: "POST", path: "/emails/{segmentEmailId}/send", success: true},
			"company/create":         {method: "POST", path: "/companies/new", body: "name:companyname @additionalFields *lower", out: "company", post: mauticSimplify},
			"company/update":         {method: "PATCH", path: "/companies/{companyId}/edit", body: "@updateFields *lower", out: "company", post: mauticSimplify},
			"company/get":            {path: "/companies/{companyId}", out: "company", post: mauticSimplify},
			"company/delete":         {method: "DELETE", path: "/companies/{companyId}/delete", out: "company", post: mauticSimplify},
			"company/getAll":         {run: mauticList("/companies", "companies")},
		},
	})
}

func basicHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// mauticJSONBody sends the raw JSON body when jsonParameters is set.
func mauticJSONBody(ec *execCtx, i int, r *restRequest) error {
	if js, _ := ec.boolean(i, "jsonParameters", false); js {
		v, _ := ec.param(i, "bodyJson", "{}")
		x, err := restConv("json", v)
		if err != nil {
			return permanentError{"Mautic: Body JSON is invalid"}
		}
		r.body = asMap(x)
	}
	return nil
}

// mauticSimplify returns the contact's flat field values unless
// options.rawData is set, as the n8n node does.
func mauticSimplify(ec *execCtx, i int, v interface{}) (interface{}, error) {
	if toBool(fieldsOf(ec, i, "options")["rawData"]) {
		return v, nil
	}
	if all := asMap(jpath(v, "fields.all")); len(all) > 0 {
		return all, nil
	}
	return v, nil
}

// mauticList pages through a Mautic list endpoint, which returns records
// keyed by id.
func mauticList(path, key string) func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	return func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
		all, _ := ec.boolean(i, "returnAll", false)
		limit, _ := ec.num(i, "limit", 30)
		q := url.Values{}
		opts := fieldsOf(ec, i, "options")
		for k, qk := range map[string]string{"search": "search", "orderBy": "orderBy", "orderByDir": "orderByDir", "publishedOnly": "publishedOnly", "minimal": "minimal"} {
			if v := opts[k]; !restEmpty(v) {
				q.Set(qk, toString(v))
			}
		}
		var out []Item
		for start := 0; start < 100000; {
			size := 30
			if !all && int(limit)-len(out) < size {
				size = int(limit) - len(out)
			}
			if size <= 0 {
				break
			}
			q.Set("start", strconv.Itoa(start))
			q.Set("limit", strconv.Itoa(size))
			v, err := c.do(ec, &restRequest{method: "GET", path: path, query: q})
			if err != nil {
				return nil, err
			}
			recs := asMap(asMap(v)[key])
			ids := make([]string, 0, len(recs))
			for id := range recs {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(a, b int) bool { x, _ := strconv.Atoi(ids[a]); y, _ := strconv.Atoi(ids[b]); return x < y })
			for _, id := range ids {
				r, _ := mauticSimplify(ec, i, recs[id])
				out = append(out, Item{JSON: asMap(r)})
			}
			start += len(ids)
			total, _ := toFloat(asMap(v)["total"])
			if len(ids) == 0 || float64(start) >= total {
				break
			}
		}
		return out, nil
	}
}

// ---------------------------------------------------------------- Google Contacts

var googleContactFields = []string{"addresses", "biographies", "birthdays", "coverPhotos", "emailAddresses", "events", "genders", "imClients", "interests", "locales", "memberships", "metadata", "names", "nicknames", "occupations", "organizations", "phoneNumbers", "photos", "relations", "residences", "sipAddresses", "skills", "urls", "userDefined"}

func registerGoogleContacts() {
	registerREST("googleContacts", &restService{
		defResource: "contact",
		defOps:      map[string]string{"contact": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			h, err := googleAuthDef(ec, i, "https://www.googleapis.com/auth/contacts", "googleContactsOAuth2Api", "oAuth2")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("googleContacts", "https://people.googleapis.com/v1"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"contact/create": {run: gcCreate},
			"contact/get":    {run: gcGet},
			"contact/getAll": {run: gcGetAll},
			"contact/update": {run: gcUpdate},
			"contact/delete": {method: "DELETE", path: "/people/{contactId}:deleteContact", success: true, build: gcIDPath},
		},
	})
}

func gcIDPath(ec *execCtx, i int, r *restRequest) error {
	r.path = strings.Replace(r.path, "/people/people/", "/people/", 1)
	return nil
}

func gcPersonFields(ec *execCtx, i int) string {
	v, _ := ec.param(i, "fields", nil)
	var fs []string
	for _, f := range asList(v) {
		if toString(f) == "*" {
			return strings.Join(googleContactFields, ",")
		}
		fs = append(fs, toString(f))
	}
	if len(fs) == 0 {
		return "names,emailAddresses,phoneNumbers"
	}
	return strings.Join(fs, ",")
}

// gcBody maps the node's contact fields to a People API person.
func gcBody(f map[string]interface{}) map[string]interface{} {
	p := map[string]interface{}{}
	name := map[string]interface{}{}
	for _, k := range []string{"givenName", "familyName", "middleName", "honorificPrefix", "honorificSuffix"} {
		if v := toString(f[k]); v != "" {
			name[k] = v
		}
	}
	if len(name) > 0 {
		p["names"] = []interface{}{name}
	}
	rows := func(coll, values string, conv func(m map[string]interface{}) map[string]interface{}) []interface{} {
		var out []interface{}
		for _, r := range asList(asMap(f[coll])[values]) {
			out = append(out, conv(asMap(r)))
		}
		return out
	}
	typed := func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"type": m["type"], "value": m["value"]}
	}
	if l := rows("emailsUi", "emailsValues", typed); l != nil {
		p["emailAddresses"] = l
	}
	if l := rows("phoneUi", "phoneValues", typed); l != nil {
		p["phoneNumbers"] = l
	}
	if l := rows("companyUi", "companyValues", func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"current": m["current"], "domain": m["domain"], "title": m["title"], "name": m["name"]}
	}); l != nil {
		p["organizations"] = l
	}
	if l := rows("addressesUi", "addressesValues", func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"streetAddress": m["streetAddress"], "city": m["city"], "region": m["region"], "countryCode": m["countryCode"], "postalCode": m["postalCode"], "type": m["type"]}
	}); l != nil {
		p["addresses"] = l
	}
	if l := rows("relationsUi", "relationsValues", func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"person": m["person"], "type": m["type"]}
	}); l != nil {
		p["relations"] = l
	}
	if l := rows("customFieldsUi", "customFieldsValues", func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"key": m["key"], "value": m["value"]}
	}); l != nil {
		p["userDefined"] = l
	}
	dateOf := func(s string) map[string]interface{} {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			if t, err = time.Parse("2006-01-02", s[:min(10, len(s))]); err != nil {
				return nil
			}
		}
		return map[string]interface{}{"year": t.Year(), "month": int(t.Month()), "day": t.Day()}
	}
	if l := rows("eventsUi", "eventsValues", func(m map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"date": dateOf(toString(m["date"])), "type": m["type"]}
	}); l != nil {
		p["events"] = l
	}
	if b := toString(f["birthday"]); b != "" {
		if d := dateOf(b); d != nil {
			p["birthdays"] = []interface{}{map[string]interface{}{"date": d}}
		}
	}
	if n := toString(f["notes"]); n != "" {
		p["biographies"] = []interface{}{map[string]interface{}{"value": n, "contentType": "TEXT_PLAIN"}}
	}
	var groups []interface{}
	for _, g := range asList(f["group"]) {
		groups = append(groups, map[string]interface{}{"contactGroupMembership": map[string]interface{}{"contactGroupResourceName": g}})
	}
	if groups != nil {
		p["memberships"] = groups
	}
	return p
}

// gcClean simplifies a person like n8n's cleanData.
func gcClean(v interface{}) map[string]interface{} {
	p := asMap(v)
	out := map[string]interface{}{}
	for k, x := range p {
		switch k {
		case "metadata":
		case "names":
			n := asMap(firstElem(x))
			delete(n, "metadata")
			out[k] = n
		case "photos":
			var urls []interface{}
			for _, ph := range asList(x) {
				urls = append(urls, asMap(ph)["url"])
			}
			out[k] = urls
		case "biographies":
			out[k] = asMap(firstElem(x))["value"]
		case "emailAddresses", "phoneNumbers", "relations", "events", "addresses":
			m := map[string]interface{}{}
			for _, e := range asList(x) {
				em := asMap(e)
				t := firstNonEmpty(toString(em["type"]), "other")
				val := em["value"]
				if val == nil {
					val = firstSet(em["formattedValue"], em["person"], em["date"])
				}
				m[t] = val
			}
			out[k] = m
		case "memberships":
			var gs []interface{}
			for _, e := range asList(x) {
				gs = append(gs, jpath(e, "contactGroupMembership.contactGroupResourceName"))
			}
			out[k] = gs
		default:
			out[k] = x
		}
	}
	if rn := toString(p["resourceName"]); rn != "" {
		out["contactId"] = strings.TrimPrefix(rn, "people/")
	}
	return out
}

func firstElem(v interface{}) interface{} {
	if l := asList(v); len(l) > 0 {
		return l[0]
	}
	return nil
}

func firstSet(vs ...interface{}) interface{} {
	for _, v := range vs {
		if v != nil {
			return v
		}
	}
	return nil
}

func gcWithID(v interface{}) map[string]interface{} {
	m := asMap(v)
	if rn := toString(m["resourceName"]); rn != "" {
		m["contactId"] = strings.TrimPrefix(rn, "people/")
	}
	return m
}

func gcCreate(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	f := fieldsOf(ec, i, "additionalFields")
	for _, k := range []string{"givenName", "familyName"} {
		if v, _ := ec.str(i, k, ""); v != "" {
			f[k] = v
		}
	}
	q := url.Values{"personFields": {strings.Join(googleContactFields, ",")}}
	v, err := c.do(ec, &restRequest{method: "POST", path: "/people:createContact", query: q, body: gcBody(f)})
	if err != nil {
		return nil, err
	}
	return []Item{{JSON: gcWithID(v)}}, nil
}

func gcGet(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	id, _ := ec.str(i, "contactId", "")
	v, err := c.do(ec, &restRequest{method: "GET", path: "/people/" + url.PathEscape(strings.TrimPrefix(id, "people/")), query: url.Values{"personFields": {gcPersonFields(ec, i)}}})
	if err != nil {
		return nil, err
	}
	if raw, _ := ec.boolean(i, "rawData", false); raw {
		return []Item{{JSON: gcWithID(v)}}, nil
	}
	return []Item{{JSON: gcClean(v)}}, nil
}

func gcGetAll(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	all, _ := ec.boolean(i, "returnAll", false)
	limit, _ := ec.num(i, "limit", 50)
	raw, _ := ec.boolean(i, "rawData", false)
	useQuery, _ := ec.boolean(i, "useQuery", false)
	q := url.Values{}
	path, key := "/people/me/connections", "connections"
	if useQuery {
		s, _ := ec.str(i, "query", "")
		path, key = "/people:searchContacts", "results"
		q.Set("query", s)
		q.Set("readMask", gcPersonFields(ec, i))
		q.Set("pageSize", "30")
	} else {
		q.Set("personFields", gcPersonFields(ec, i))
		q.Set("pageSize", "1000")
		if s := toString(fieldsOf(ec, i, "options")["sortOrder"]); s != "" {
			q.Set("sortOrder", s)
		}
	}
	var out []Item
	for n := 0; n < 1000; n++ {
		v, err := c.do(ec, &restRequest{method: "GET", path: path, query: q})
		if err != nil {
			return nil, err
		}
		for _, x := range asList(asMap(v)[key]) {
			if useQuery {
				x = asMap(x)["person"]
			}
			if raw {
				out = append(out, Item{JSON: gcWithID(x)})
			} else {
				out = append(out, Item{JSON: gcClean(x)})
			}
			if !all && len(out) >= int(limit) {
				return out, nil
			}
		}
		next := toString(asMap(v)["nextPageToken"])
		if next == "" || useQuery {
			break
		}
		q.Set("pageToken", next)
	}
	return out, nil
}

func gcUpdate(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	id, _ := ec.str(i, "contactId", "")
	path := "/people/" + url.PathEscape(strings.TrimPrefix(id, "people/"))
	f := fieldsOf(ec, i, "updateFields")
	body := gcBody(f)
	var fields []string
	for k := range body {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	if len(fields) == 0 {
		return nil, permanentError{"Google Contacts: no fields to update"}
	}
	cur, err := c.do(ec, &restRequest{method: "GET", path: path, query: url.Values{"personFields": {strings.Join(fields, ",")}}})
	if err != nil {
		return nil, err
	}
	body["etag"] = asMap(cur)["etag"]
	v, err := c.do(ec, &restRequest{method: "PATCH", path: path + ":updateContact", query: url.Values{"updatePersonFields": {strings.Join(fields, ",")}, "personFields": {strings.Join(googleContactFields, ",")}}, body: body})
	if err != nil {
		return nil, err
	}
	if raw, _ := ec.boolean(i, "rawData", false); raw {
		return []Item{{JSON: gcWithID(v)}}, nil
	}
	return []Item{{JSON: gcClean(v)}}, nil
}

// ---------------------------------------------------------------- NASA

var nasaDonki = map[string]string{
	"donkiCoronalMassEjection": "/DONKI/CME", "donkiGeomagneticStorm": "/DONKI/GST", "donkiInterplanetaryShock": "/DONKI/IPS",
	"donkiSolarFlare": "/DONKI/FLR", "donkiSolarEnergeticParticle": "/DONKI/SEP", "donkiMagnetopauseCrossing": "/DONKI/MPC",
	"donkiRadiationBeltEnhancement": "/DONKI/RBE", "donkiHighSpeedStream": "/DONKI/HSS", "donkiWsaEnlilSimulation": "/DONKI/WSAEnlilSimulations",
	"donkiNotifications": "/DONKI/notifications",
}

func registerNASA() {
	ops := map[string]*restOp{
		"astronomyPictureOfTheDay/get": {run: nasaAPOD},
		"asteroidNeoFeed/get": {path: "/neo/rest/v1/feed", query: "@additionalFields startDate:start_date endDate:end_date", post: func(_ *execCtx, _ int, v interface{}) (interface{}, error) {
			days := asMap(asMap(v)["near_earth_objects"])
			keys := make([]string, 0, len(days))
			for k := range days {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var out []interface{}
			for _, k := range keys {
				out = append(out, asList(days[k])...)
			}
			return out, nil
		}},
		"asteroidNeoLookup/get": {path: "/neo/rest/v1/neo/{asteroidId}", post: func(ec *execCtx, i int, v interface{}) (interface{}, error) {
			if !toBool(fieldsOf(ec, i, "additionalFields")["includeCloseApproachData"]) {
				delete(asMap(v), "close_approach_data")
			}
			return v, nil
		}},
		"asteroidNeoBrowse/getAll": {path: "/neo/rest/v1/neo/browse", list: true, listKey: "near_earth_objects", pager: pageNumber("page", 0), sizeParam: "size", maxSize: 20},
		"earthImagery/get":         {run: nasaEarthImagery},
		"earthAssets/get":          {path: "/planetary/earth/assets", query: "lat lon @additionalFields date dim", build: nasaDefaultDate},
	}
	defOps := map[string]string{"astronomyPictureOfTheDay": "get", "asteroidNeoFeed": "get", "asteroidNeoLookup": "get", "asteroidNeoBrowse": "getAll", "earthImagery": "get", "earthAssets": "get"}
	for res, p := range nasaDonki {
		q := "@additionalFields startDate endDate"
		switch res {
		case "donkiInterplanetaryShock":
			q += " location catalog"
		case "donkiNotifications":
			q += " type"
		}
		ops[res+"/get"] = &restOp{path: p, query: q}
		defOps[res] = "get"
	}
	registerREST("nasa", &restService{
		defResource: "astronomyPictureOfTheDay",
		defOps:      defOps,
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("nasaApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("nasa", "https://api.nasa.gov"), query: url.Values{"api_key": {credString(c, "api_key", "apiKey")}}}, nil
		},
		ops: ops,
	})
}

func nasaDefaultDate(ec *execCtx, i int, r *restRequest) error {
	if r.query.Get("date") == "" {
		r.query.Set("date", time.Now().UTC().Format("2006-01-02"))
	}
	if d := r.query.Get("date"); len(d) > 10 {
		r.query.Set("date", d[:10])
	}
	return nil
}

func nasaAPOD(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	f := fieldsOf(ec, i, "additionalFields")
	q := url.Values{}
	if d := toString(f["date"]); d != "" {
		q.Set("date", d[:min(10, len(d))])
	}
	v, err := c.do(ec, &restRequest{method: "GET", path: "/planetary/apod", query: q})
	if err != nil {
		return nil, err
	}
	it := Item{JSON: asMap(v)}
	if dl, _ := ec.boolean(i, "download", true); dl {
		if toString(it.JSON["media_type"]) == "video" {
			return nil, permanentError{"NASA: the astronomy picture of the day is a video and cannot be downloaded; turn off Download"}
		}
		u := toString(it.JSON["url"])
		if toBool(f["highResolution"]) && toString(it.JSON["hdurl"]) != "" {
			u = toString(it.JSON["hdurl"])
		}
		res, err := ec.doRequest("GET", u, map[string]string{}, nil, "")
		if err != nil {
			return nil, err
		}
		prop, _ := ec.str(i, "binaryPropertyName", "data")
		name := u[strings.LastIndex(u, "/")+1:]
		it.Binary = map[string]interface{}{prop: binaryData(res.body, firstNonEmpty(res.contentType, sniffMime(res.body)), name)}
	}
	return []Item{it}, nil
}

func nasaEarthImagery(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	r, err := (&restOp{path: "/planetary/earth/imagery", query: "lat lon @additionalFields date dim"}).request(ec, i)
	if err != nil {
		return nil, err
	}
	_ = nasaDefaultDate(ec, i, r)
	res, err := cmtyRaw(ec, c, "GET", r.path, r.query, nil, "")
	if err != nil {
		return nil, err
	}
	if res.status >= 400 {
		return nil, fmt.Errorf("NASA: %d %s", res.status, strings.TrimSpace(string(res.body)))
	}
	prop, _ := ec.str(i, "binaryPropertyName", "data")
	return []Item{{JSON: map[string]interface{}{}, Binary: map[string]interface{}{prop: binaryData(res.body, firstNonEmpty(res.contentType, "image/png"), "image.png")}}}, nil
}

// ---------------------------------------------------------------- Google Business Profile

func registerGoogleBusinessProfile() {
	registerREST("googleBusinessProfile", &restService{
		defResource: "post",
		defOps:      map[string]string{"post": "create", "review": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			h, err := googleAuthDef(ec, i, "https://www.googleapis.com/auth/business.manage", "googleBusinessProfileOAuth2Api", "oAuth2")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("googleBusinessProfile", "https://mybusiness.googleapis.com/v4"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"post/create": {method: "POST", path: "/{+account}/{+location}/localPosts", build: gbpPostBody, post: gbpStrip},
			"post/get":    {path: "/{+post}", build: gbpFullName("post", "localPosts")},
			"post/delete": {method: "DELETE", path: "/{+post}", success: true, build: gbpFullName("post", "localPosts")},
			"post/getAll": {path: "/{+account}/{+location}/localPosts", list: true, listKey: "localPosts", pager: cursorPager("nextPageToken", "pageToken"), sizeParam: "pageSize", maxSize: 100},
			"post/update": {method: "PATCH", path: "/{+post}", build: func(ec *execCtx, i int, r *restRequest) error {
				if err := gbpFullName("post", "localPosts")(ec, i, r); err != nil {
					return err
				}
				f := fieldsOf(ec, i, "additionalOptions")
				var mask []string
				for k, v := range f {
					switch k {
					case "summary", "languageCode":
						r.body[k] = v
						mask = append(mask, k)
					}
				}
				sort.Strings(mask)
				r.query.Set("updateMask", strings.Join(mask, ","))
				return nil
			}},
			"review/get":         {path: "/{+review}", build: gbpFullName("review", "reviews")},
			"review/getAll":      {path: "/{+account}/{+location}/reviews", list: true, listKey: "reviews", pager: cursorPager("nextPageToken", "pageToken"), sizeParam: "pageSize", maxSize: 50},
			"review/reply":       {method: "PUT", path: "/{+review}/reply", body: "reply:comment", build: gbpFullName("review", "reviews")},
			"review/deleteReply": {method: "DELETE", path: "/{+review}/reply", success: true, build: gbpFullName("review", "reviews")},
		},
	})
}

// gbpFullName expands a bare post/review id into its resource name.
func gbpFullName(param, coll string) func(ec *execCtx, i int, r *restRequest) error {
	return func(ec *execCtx, i int, r *restRequest) error {
		v := strings.Trim(rlValue(ec.mustParam(i, param)), "/")
		if strings.Contains(v, "/"+coll+"/") {
			return nil
		}
		acct := strings.Trim(rlValue(ec.mustParam(i, "account")), "/")
		loc := strings.Trim(rlValue(ec.mustParam(i, "location")), "/")
		r.path = strings.Replace(r.path, "/"+v, "/"+acct+"/"+loc+"/"+coll+"/"+v, 1)
		return nil
	}
}

func (ec *execCtx) mustParam(i int, k string) interface{} {
	v, _ := ec.param(i, k, "")
	return v
}

func gbpPostBody(ec *execCtx, i int, r *restRequest) error {
	t, _ := ec.str(i, "postType", "STANDARD")
	s, _ := ec.str(i, "summary", "")
	r.body["topicType"] = t
	r.body["summary"] = s
	r.body["languageCode"] = "en-US"
	f := fieldsOf(ec, i, "additionalOptions")
	if v := toString(f["languageCode"]); v != "" {
		r.body["languageCode"] = v
	}
	if a := toString(f["callToActionType"]); a != "" {
		r.body["callToAction"] = map[string]interface{}{"actionType": a, "url": f["url"]}
	}
	if t == "EVENT" || t == "OFFER" {
		ev := map[string]interface{}{"title": firstNonEmpty(toString(f["title"]), s)}
		sched := map[string]interface{}{}
		for k, n := range map[string]string{"startDateTime": "start", "endDateTime": "end"} {
			if d := toString(f[k]); d != "" {
				if tm, err := time.Parse(time.RFC3339, d); err == nil {
					sched[n+"Date"] = map[string]interface{}{"year": tm.Year(), "month": int(tm.Month()), "day": tm.Day()}
					sched[n+"Time"] = map[string]interface{}{"hours": tm.Hour(), "minutes": tm.Minute()}
				}
			}
		}
		if len(sched) > 0 {
			ev["schedule"] = sched
		}
		r.body["event"] = ev
	}
	if t == "OFFER" {
		r.body["offer"] = map[string]interface{}{"couponCode": f["couponCode"], "redeemOnlineUrl": f["redeemOnlineUrl"], "termsConditions": f["termsConditions"]}
	}
	if t == "ALERT" {
		r.body["alertType"] = firstNonEmpty(toString(f["alertType"]), "COVID_19")
	}
	return nil
}

func gbpStrip(_ *execCtx, _ int, v interface{}) (interface{}, error) { return v, nil }

// ---------------------------------------------------------------- Home Assistant

func registerHomeAssistant() {
	attrs := func(coll string) func(ec *execCtx, i int, r *restRequest) error {
		return func(ec *execCtx, i int, r *restRequest) error {
			v, _ := ec.param(i, coll, nil)
			if m := rowsToMap(asMap(v)["attributes"], "name", "value"); len(m) > 0 {
				if coll == "stateAttributes" {
					r.body["attributes"] = m
				} else {
					for k, x := range m {
						r.body[k] = x
					}
				}
			}
			return nil
		}
	}
	registerREST("homeAssistant", &restService{
		defResource: "config",
		defOps:      map[string]string{"config": "get", "state": "get", "service": "getAll", "event": "getAll", "log": "getErroLogs", "template": "create", "history": "getAll", "cameraProxy": "getScreenshot"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("homeAssistantApi")
			if err != nil {
				return nil, err
			}
			scheme := "http"
			if toBool(c["ssl"]) {
				scheme = "https"
			}
			host := strings.TrimRight(credString(c, "host"), "/")
			if !strings.Contains(host, "://") {
				host = scheme + "://" + host
				if p := credString(c, "port"); p != "" {
					host += ":" + p
				}
			}
			return &restClient{base: ec.apiBase("homeAssistant", host+"/api"), headers: map[string]string{"Authorization": "Bearer " + credString(c, "accessToken")}}, nil
		},
		ops: map[string]*restOp{
			"config/get":     {path: "/config"},
			"config/check":   {method: "POST", path: "/config/core/check_config"},
			"state/get":      {path: "/states/{entityId}"},
			"state/getAll":   {path: "/states", list: true},
			"state/upsert":   {method: "POST", path: "/states/{entityId}", body: "state", build: attrs("stateAttributes")},
			"service/getAll": {path: "/services", list: true},
			"service/call": {method: "POST", path: "/services/{domain}/{service}", build: attrs("serviceAttributes"), post: func(_ *execCtx, _ int, v interface{}) (interface{}, error) {
				if l := asList(v); len(l) == 0 {
					return map[string]interface{}{"success": true}, nil
				}
				return v, nil
			}},
			"event/getAll": {path: "/events", list: true},
			"event/create": {method: "POST", path: "/events/{eventType}", build: attrs("eventAttributes")},
			"log/getErroLogs": {run: func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
				res, err := cmtyRaw(ec, c, "GET", "/error_log", nil, nil, "")
				if err != nil {
					return nil, err
				}
				return []Item{{JSON: map[string]interface{}{"errorLog": string(res.body)}}}, nil
			}},
			"log/getLogbookEntries": {path: "/logbook/{startTime?}", query: "@additionalFields endTime:end_time entityId:entity", list: true},
			"template/create":       {run: haTemplate},
			"history/getAll":        {path: "/history/period/{startTime?}", query: "@additionalFields endTime:end_time entityIds:filter_entity_id minimalResponse:minimal_response significantChangesOnly:significant_changes_only", list: true},
			"cameraProxy/getScreenshot": {run: func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
				id, _ := ec.str(i, "cameraEntityId", "")
				res, err := cmtyRaw(ec, c, "GET", "/camera_proxy/"+url.PathEscape(id), nil, nil, "")
				if err != nil {
					return nil, err
				}
				prop, _ := ec.str(i, "binaryPropertyName", "data")
				return []Item{{JSON: map[string]interface{}{}, Binary: map[string]interface{}{prop: binaryData(res.body, firstNonEmpty(res.contentType, "image/jpeg"), "screenshot.jpg")}}}, nil
			}},
		},
	})
}

func haTemplate(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	t, _ := ec.str(i, "template", "")
	res, err := cmtyRaw(ec, c, "POST", "/template", nil, []byte(mustJSON(map[string]interface{}{"template": t})), "application/json")
	if err != nil {
		return nil, err
	}
	return []Item{{JSON: map[string]interface{}{"renderedTemplate": string(res.body)}}}, nil
}

// ---------------------------------------------------------------- Pushover

func registerPushover() {
	registerREST("pushover", &restService{
		defResource: "message",
		defOps:      map[string]string{"message": "push"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("pushoverApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("pushover", "https://api.pushover.net/1"), query: url.Values{"token": {credString(c, "apiKey")}}}, nil
		},
		ops: map[string]*restOp{"message/push": {run: pushoverPush}},
	})
}

func pushoverPush(ec *execCtx, i int, it Item, c *restClient) ([]Item, error) {
	user, _ := ec.str(i, "userKey", "")
	msg, _ := ec.str(i, "message", "")
	prio, _ := ec.num(i, "priority", -2)
	fields := [][2]string{{"token", c.query.Get("token")}, {"user", user}, {"message", msg}, {"priority", strconv.Itoa(int(prio))}}
	if prio == 2 {
		r, _ := ec.num(i, "retry", 30)
		e, _ := ec.num(i, "expire", 30)
		fields = append(fields, [2]string{"retry", strconv.Itoa(int(r))}, [2]string{"expire", strconv.Itoa(int(e))})
	}
	f := fieldsOf(ec, i, "additionalFields")
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var files []cmtyFile
	for _, k := range keys {
		v := f[k]
		switch k {
		case "attachmentsUi":
			for _, a := range asList(asMap(v)["attachmentsValues"]) {
				prop := firstNonEmpty(toString(asMap(a)["binaryPropertyName"]), "data")
				data, mt, name, err := binaryOf(it, prop)
				if err != nil {
					return nil, err
				}
				files = append(files, cmtyFile{field: "attachment", name: firstNonEmpty(name, "attachment"), mimeType: mt, data: data})
			}
		case "html":
			if toBool(v) {
				fields = append(fields, [2]string{"html", "1"})
			}
		case "timestamp":
			if tm, err := time.Parse(time.RFC3339, toString(v)); err == nil {
				fields = append(fields, [2]string{"timestamp", strconv.FormatInt(tm.Unix(), 10)})
			}
		default:
			if !restEmpty(v) {
				fields = append(fields, [2]string{k, toString(v)})
			}
		}
	}
	body, ct := cmtyMultipart(fields, files...)
	res, err := cmtyRaw(ec, &restClient{base: c.base}, "POST", "/messages.json", nil, body, ct)
	if err != nil {
		return nil, err
	}
	v := decodeJSONBody(res.body)
	if res.status >= 400 {
		return nil, fmt.Errorf("pushover: %d %s", res.status, mustJSON(v))
	}
	return restItems(v), nil
}

// ---------------------------------------------------------------- Dropcontact

func registerDropcontact() {
	registerREST("dropcontact", &restService{
		defResource: "contact",
		defOps:      map[string]string{"contact": "enrich"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("dropcontactApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("dropcontact", "https://api.dropcontact.io"), headers: map[string]string{"X-Access-Token": credString(c, "apiKey")}}, nil
		},
		ops: map[string]*restOp{
			"contact/enrich": {method: "POST", path: "/batch", build: func(ec *execCtx, i int, r *restRequest) error {
				d := map[string]interface{}{}
				if err := ec.restFields(i, "email @additionalFields firstName:first_name lastName:last_name fullName:full_name phone company website numSiren:num_siren companySiren:siren", d); err != nil {
					return err
				}
				r.body["data"] = []interface{}{d}
				o := fieldsOf(ec, i, "options")
				r.body["siren"] = toBool(o["siren"])
				r.body["language"] = firstNonEmpty(toString(o["language"]), "en")
				return nil
			}},
			"contact/fetchRequest": {path: "/batch/{requestId}", post: func(ec *execCtx, i int, v interface{}) (interface{}, error) {
				if s, _ := ec.boolean(i, "simplify", false); s {
					if !toBool(asMap(v)["success"]) {
						return v, nil
					}
					return asList(asMap(v)["data"]), nil
				}
				return v, nil
			}},
		},
	})
}

// ---------------------------------------------------------------- lemlist

func registerLemlist() {
	enrichQuery := "findEmail verifyEmail linkedinEnrichment findPhone"
	registerREST("lemlist", &restService{
		defResource: "activity",
		defOps:      map[string]string{"activity": "getAll", "campaign": "getAll", "lead": "create", "team": "get", "unsubscribe": "add", "enrich": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("lemlistApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("lemlist", "https://api.lemlist.com/api"), headers: map[string]string{"Authorization": basicHeader("", credString(c, "apiKey"))}}, nil
		},
		ops: map[string]*restOp{
			"activity/getAll":     {path: "/activities", query: "@filters campaignId type isFirst leadId contactId", list: true, pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100},
			"campaign/getAll":     {path: "/campaigns", list: true, pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100, query: "@filters status"},
			"campaign/getStats":   {path: "/v2/campaigns/{campaignId}/stats", query: "startDate endDate timezone"},
			"lead/create":         {method: "POST", path: "/campaigns/{campaignId}/leads/{email}", body: "@additionalFields companyName firstName lastName icebreaker phone picture linkedinUrl companyDomain", build: lemlistDedupe},
			"lead/get":            {path: "/leads/{email}"},
			"lead/unsubscribe":    {method: "DELETE", path: "/campaigns/{campaignId}/leads/{email}"},
			"lead/delete":         {method: "DELETE", path: "/campaigns/{campaignId}/leads/{email}", query: "=action:remove"},
			"team/get":            {path: "/team"},
			"team/getCredits":     {path: "/team/credits"},
			"unsubscribe/add":     {method: "POST", path: "/unsubscribes/{email}"},
			"unsubscribe/delete":  {method: "DELETE", path: "/unsubscribes/{email}"},
			"unsubscribe/getAll":  {path: "/unsubscribes", list: true, pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100},
			"enrich/get":          {path: "/enrich/{enrichId}"},
			"enrich/enrichLead":   {method: "POST", path: "/leads/{leadId}/enrich", query: enrichQuery},
			"enrich/enrichPerson": {method: "POST", path: "/enrich", query: enrichQuery + " @additionalFields email firstName lastName linkedinUrl companyName companyDomain"},
		},
	})
}

func lemlistDedupe(ec *execCtx, i int, r *restRequest) error {
	if d := fieldsOf(ec, i, "additionalFields")["deduplicate"]; d != nil {
		delete(r.body, "deduplicate")
		r.query.Set("deduplicate", strconv.FormatBool(toBool(d)))
	}
	return nil
}
