package n8n

import (
	"strings"
	"testing"
)

// TestWave5Connectors runs each new service node against a fake API and
// checks the request it sends and the items it returns.
func TestWave5Connectors(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.HasSuffix(r.path, "/api/contacts/new"):
			return 200, "application/json", map[string]interface{}{"contact": map[string]interface{}{"id": 7, "fields": map[string]interface{}{"all": map[string]interface{}{"id": 7, "email": "a@b.c"}}}}
		case strings.HasSuffix(r.path, "/api/contacts") && r.method == "GET":
			return 200, "application/json", map[string]interface{}{"total": 2, "contacts": map[string]interface{}{"2": map[string]interface{}{"fields": map[string]interface{}{"all": map[string]interface{}{"id": 2}}}, "1": map[string]interface{}{"fields": map[string]interface{}{"all": map[string]interface{}{"id": 1}}}}}
		case r.path == "/people:createContact":
			return 200, "application/json", map[string]interface{}{"resourceName": "people/c42", "names": []interface{}{map[string]interface{}{"givenName": "Ada"}}}
		case r.path == "/people/me/connections":
			return 200, "application/json", map[string]interface{}{"connections": []interface{}{map[string]interface{}{"resourceName": "people/c1", "names": []interface{}{map[string]interface{}{"displayName": "Ada", "metadata": map[string]interface{}{}}}, "emailAddresses": []interface{}{map[string]interface{}{"type": "work", "value": "a@x.io"}}}}}
		case r.path == "/neo/rest/v1/feed":
			return 200, "application/json", map[string]interface{}{"near_earth_objects": map[string]interface{}{"2024-01-02": []interface{}{map[string]interface{}{"id": "b"}}, "2024-01-01": []interface{}{map[string]interface{}{"id": "a"}}}}
		case strings.HasPrefix(r.path, "/services/"):
			return 200, "application/json", []interface{}{}
		case r.path == "/messages.json":
			return 200, "application/json", map[string]interface{}{"status": 1, "request": "r1"}
		case r.path == "/batch":
			return 200, "application/json", map[string]interface{}{"request_id": "q1", "success": true}
		case strings.HasPrefix(r.path, "/incidents"):
			return 201, "application/json", map[string]interface{}{"incident": map[string]interface{}{"id": "P1", "title": "Down"}}
		case r.path == "/chart":
			return 200, "image/png", "PNGDATA"
		case strings.HasPrefix(r.path, "/collections/"):
			return 200, "application/json", map[string]interface{}{"id": "it1"}
		case strings.HasSuffix(r.path, "/messages") && strings.HasPrefix(r.path, "/spaces/"):
			return 200, "application/json", map[string]interface{}{"name": "spaces/S/messages/M"}
		case strings.HasSuffix(r.path, "/api/v1/users"):
			return 201, "application/json", map[string]interface{}{"id": 3, "firstname": "Ada"}
		case strings.HasSuffix(r.path, "/predict"):
			return 201, "application/json", map[string]interface{}{"document": map[string]interface{}{"inference": map[string]interface{}{"prediction": map[string]interface{}{"total_amount": map[string]interface{}{"value": 12.5}, "taxes": []interface{}{map[string]interface{}{"value": 2.1}}}}}}
		case strings.HasSuffix(r.path, "/chat.postMessage"):
			return 200, "application/json", map[string]interface{}{"success": true}
		case r.path == "/images":
			return 202, "application/json", map[string]interface{}{"uid": "u1", "status": "pending"}
		case r.path == "/getAccountDetails":
			return 200, "application/json", map[string]interface{}{"stat": "ok", "account": map[string]interface{}{"email": "a@b.c"}}
		case r.path == "/activities":
			return 201, "application/json", map[string]interface{}{"id": 9}
		case strings.Contains(r.path, "/send/m.room.message/"):
			return 200, "application/json", map[string]interface{}{"event_id": "$e"}
		case r.path == "/sms":
			return 200, "application/json", map[string]interface{}{"success": "100"}
		case r.path == "/subscribers":
			return 200, "application/json", map[string]interface{}{"id": 5, "email": "a@b.c"}
		case r.path == "/simple/price":
			return 200, "application/json", map[string]interface{}{"bitcoin": map[string]interface{}{"usd": 1}, "ethereum": map[string]interface{}{"usd": 2}}
		case r.path == "/v3.1/send":
			return 200, "application/json", map[string]interface{}{"Messages": []interface{}{map[string]interface{}{"Status": "success"}}}
		case r.path == "/tickets":
			return 201, "application/json", map[string]interface{}{"id": 11}
		case strings.HasSuffix(r.path, "/tasks"):
			return 201, "application/json", map[string]interface{}{"id": "t1", "title": "Buy"}
		case r.path == "/raindrop":
			return 200, "application/json", map[string]interface{}{"item": map[string]interface{}{"_id": 1}}
		case r.path == "/me":
			return 200, "application/json", map[string]interface{}{"data": map[string]interface{}{"id": "U1"}}
		case r.path == "/users/U1/posts":
			return 201, "application/json", map[string]interface{}{"data": map[string]interface{}{"id": "p1"}}
		case r.path == "/volumes":
			return 200, "application/json", map[string]interface{}{"items": []interface{}{map[string]interface{}{"id": "v1"}}}
		case r.path == "/documents:analyzeSentiment":
			return 200, "application/json", map[string]interface{}{"documentSentiment": map[string]interface{}{"score": 0.8}}
		case r.path == "/users" && r.method == "POST":
			return 200, "application/json", map[string]interface{}{"type": "user", "id": "iu1"}
		case strings.HasSuffix(r.path, "/projects"):
			return 201, "application/json", map[string]interface{}{"id": "pr1"}
		case strings.HasPrefix(r.path, "/campaigns/"):
			return 200, "application/json", map[string]interface{}{"_id": "l1"}
		case strings.Contains(r.path, "/localPosts"):
			return 200, "application/json", map[string]interface{}{"name": "accounts/1/locations/2/localPosts/3"}
		}
		return 404, "application/json", map[string]interface{}{"error": "unexpected " + r.method + " " + r.path}
	})
	type check struct {
		method, path string
		has          []string // substrings of query + body + auth
	}
	cases := []struct {
		name, api, node string
		version         float64
		params          map[string]interface{}
		creds           map[string]map[string]interface{}
		req             check
		out             func(out []map[string]interface{}) bool
	}{
		{"mautic create", "mautic", "mautic", 1, map[string]interface{}{"email": "a@b.c", "firstName": "Ada", "additionalFields": map[string]interface{}{"company": "X"}},
			map[string]map[string]interface{}{"mauticApi": {"url": "http://m", "username": "u", "password": "p"}},
			check{"POST", "/api/contacts/new", []string{`"firstname":"Ada"`, `"company":"X"`, "Basic dTpw"}},
			func(o []map[string]interface{}) bool { return o[0]["email"] == "a@b.c" }},
		{"mautic getAll", "mautic", "mautic", 1, map[string]interface{}{"operation": "getAll", "returnAll": true},
			map[string]map[string]interface{}{"mauticApi": {"url": "http://m", "username": "u", "password": "p"}},
			check{"GET", "/api/contacts", []string{"start=0"}},
			func(o []map[string]interface{}) bool { return len(o) == 2 && o[0]["id"] == 1.0 }},
		{"google contacts create", "googleContacts", "googleContacts", 1, map[string]interface{}{"givenName": "Ada", "familyName": "L", "additionalFields": map[string]interface{}{"emailsUi": map[string]interface{}{"emailsValues": []interface{}{map[string]interface{}{"type": "home", "value": "a@x.io"}}}}},
			map[string]map[string]interface{}{"googleContactsOAuth2Api": {"accessToken": "T"}},
			check{"POST", "/people:createContact", []string{`"givenName":"Ada"`, `"emailAddresses":[{"type":"home","value":"a@x.io"}]`, "Bearer T"}},
			func(o []map[string]interface{}) bool { return o[0]["contactId"] == "c42" }},
		{"google contacts getAll", "googleContacts", "googleContacts", 1, map[string]interface{}{"operation": "getAll", "fields": []interface{}{"names", "emailAddresses"}, "limit": 5},
			map[string]map[string]interface{}{"googleContactsOAuth2Api": {"accessToken": "T"}},
			check{"GET", "/people/me/connections", []string{"personFields=names%2CemailAddresses"}},
			func(o []map[string]interface{}) bool {
				return o[0]["contactId"] == "c1" && asMap(o[0]["emailAddresses"])["work"] == "a@x.io" && asMap(o[0]["names"])["metadata"] == nil
			}},
		{"nasa neo feed", "nasa", "nasa", 1, map[string]interface{}{"resource": "asteroidNeoFeed", "additionalFields": map[string]interface{}{"startDate": "2024-01-01"}},
			map[string]map[string]interface{}{"nasaApi": {"api_key": "K"}},
			check{"GET", "/neo/rest/v1/feed", []string{"api_key=K", "start_date=2024-01-01"}},
			func(o []map[string]interface{}) bool { return len(o) == 2 && o[0]["id"] == "a" }},
		{"home assistant call", "homeAssistant", "homeAssistant", 1, map[string]interface{}{"resource": "service", "operation": "call", "domain": "light", "service": "turn_on", "serviceAttributes": map[string]interface{}{"attributes": []interface{}{map[string]interface{}{"name": "entity_id", "value": "light.k"}}}},
			map[string]map[string]interface{}{"homeAssistantApi": {"host": "h", "accessToken": "HT"}},
			check{"POST", "/services/light/turn_on", []string{`"entity_id":"light.k"`, "Bearer HT"}},
			func(o []map[string]interface{}) bool { return o[0]["success"] == true }},
		{"pushover", "pushover", "pushover", 1, map[string]interface{}{"userKey": "U", "message": "hi", "priority": 0, "additionalFields": map[string]interface{}{"title": "T"}},
			map[string]map[string]interface{}{"pushoverApi": {"apiKey": "PK"}},
			check{"POST", "/messages.json", []string{`name="token"`, "PK", `name="title"`}},
			func(o []map[string]interface{}) bool { return o[0]["request"] == "r1" }},
		{"dropcontact", "dropcontact", "dropcontact", 1, map[string]interface{}{"email": "a@b.c", "additionalFields": map[string]interface{}{"firstName": "Ada"}},
			map[string]map[string]interface{}{"dropcontactApi": {"apiKey": "D"}},
			check{"POST", "/batch", []string{`"data":[{"email":"a@b.c","first_name":"Ada"}]`, `"language":"en"`}},
			func(o []map[string]interface{}) bool { return o[0]["request_id"] == "q1" }},
		{"lemlist lead", "lemlist", "lemlist", 1, map[string]interface{}{"resource": "lead", "operation": "create", "campaignId": "c1", "email": "a@b.c", "additionalFields": map[string]interface{}{"firstName": "Ada", "deduplicate": true}},
			map[string]map[string]interface{}{"lemlistApi": {"apiKey": "L"}},
			check{"POST", "/campaigns/c1/leads/a@b.c", []string{"deduplicate=true", `"firstName":"Ada"`, "Basic Okw="}},
			func(o []map[string]interface{}) bool { return o[0]["_id"] == "l1" }},
		{"clockify project", "clockify", "clockify", 1, map[string]interface{}{"workspaceId": "w1", "name": "P"},
			map[string]map[string]interface{}{"clockifyApi": {"apiKey": "C"}},
			check{"POST", "/workspaces/w1/projects", []string{`"name":"P"`}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "pr1" }},
		{"pagerduty incident", "pagerDuty", "pagerDuty", 1, map[string]interface{}{"title": "Down", "serviceId": "S1", "email": "ops@x.io", "additionalFields": map[string]interface{}{"urgency": "high", "details": "db"}},
			map[string]map[string]interface{}{"pagerDutyApi": {"apiToken": "PD"}},
			check{"POST", "/incidents", []string{`"service":{"id":"S1","type":"service_reference"}`, `"urgency":"high"`, `"details":"db"`, "Token token=PD"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "P1" }},
		{"webflow v2 create", "webflow", "webflow", 2, map[string]interface{}{"operation": "create", "siteId": "s", "collectionId": "c9", "live": true, "fieldsUi": map[string]interface{}{"fieldValues": []interface{}{map[string]interface{}{"fieldId": "name", "fieldValue": "X"}}}},
			map[string]map[string]interface{}{"webflowApi": {"accessToken": "W"}},
			check{"POST", "/collections/c9/items/live", []string{`"fieldData":{"name":"X"}`, `"isDraft":false`, "Bearer W"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "it1" }},
		{"google chat message", "googleChat", "googleChat", 1, map[string]interface{}{"authentication": "oAuth2", "spaceId": "spaces/S", "messageUi": map[string]interface{}{"text": "hello"}},
			map[string]map[string]interface{}{"googleChatOAuth2Api": {"accessToken": "G"}},
			check{"POST", "/spaces/S/messages", []string{`"text":"hello"`, "Bearer G"}},
			func(o []map[string]interface{}) bool { return o[0]["name"] == "spaces/S/messages/M" }},
		{"zammad user", "zammad", "zammad", 1, map[string]interface{}{"firstname": "Ada", "lastname": "L", "additionalFields": map[string]interface{}{"email": "a@b.c"}},
			map[string]map[string]interface{}{"zammadTokenAuthApi": {"baseUrl": "http://z", "accessToken": "Z"}},
			check{"POST", "/api/v1/users", []string{`"firstname":"Ada"`, `"email":"a@b.c"`, "Token token=Z"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == 3.0 }},
		{"rocketchat", "rocketchat", "rocketchat", 1, map[string]interface{}{"channel": "#g", "text": "hi", "attachments": []interface{}{map[string]interface{}{"title": "T", "imageUrl": "http://i"}}},
			map[string]map[string]interface{}{"rocketchatApi": {"domain": "http://r", "userId": "u", "authKey": "k"}},
			check{"POST", "/api/v1/chat.postMessage", []string{`"channel":"#g"`, `"image_url":"http://i"`}},
			func(o []map[string]interface{}) bool { return o[0]["success"] == true }},
		{"bannerbear", "bannerbear", "bannerbear", 1, map[string]interface{}{"templateId": "tp", "modificationsUi": map[string]interface{}{"modificationsValues": []interface{}{map[string]interface{}{"name": "title", "text": "Hi", "imageUrl": "http://i"}}}},
			map[string]map[string]interface{}{"bannerbearApi": {"apiKey": "B"}},
			check{"POST", "/images", []string{`"template":"tp"`, `"image_url":"http://i"`, "Bearer B"}},
			func(o []map[string]interface{}) bool { return o[0]["uid"] == "u1" }},
		{"uptimerobot account", "uptimeRobot", "uptimeRobot", 1, map[string]interface{}{},
			map[string]map[string]interface{}{"uptimeRobotApi": {"apiKey": "UR"}},
			check{"POST", "/getAccountDetails", []string{"api_key=UR", "format=json"}},
			func(o []map[string]interface{}) bool { return o[0]["email"] == "a@b.c" }},
		{"strava create", "strava", "strava", 1.1, map[string]interface{}{"name": "Run", "type": "Run", "startDate": "2024-01-01T08:00:00Z", "elapsedTime": 3600},
			map[string]map[string]interface{}{"stravaOAuth2Api": {"accessToken": "S"}},
			check{"POST", "/activities", []string{`"sport_type":"Run"`, `"elapsed_time":3600`, "Bearer S"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == 9.0 }},
		{"matrix message", "matrix", "matrix", 1, map[string]interface{}{"roomId": "!r:x", "text": "hi"},
			map[string]map[string]interface{}{"matrixApi": {"homeserverUrl": "http://m", "accessToken": "M"}},
			check{"PUT", "/_matrix/client/r0/rooms/!r:x/send/m.room.message/", []string{`"msgtype":"m.text"`, `"body":"hi"`, "Bearer M"}},
			func(o []map[string]interface{}) bool { return o[0]["event_id"] == "$e" }},
		{"sms77", "sms77", "sms77", 1, map[string]interface{}{"to": "+49", "message": "hi", "from": "n8n"},
			map[string]map[string]interface{}{"sms77Api": {"apiKey": "S7"}},
			check{"POST", "/sms", []string{`"text":"hi"`, `"to":"+49"`}},
			func(o []map[string]interface{}) bool { return o[0]["success"] == "100" }},
		{"mailerlite v2", "mailerLite", "mailerLite", 2, map[string]interface{}{"email": "a@b.c", "additionalFields": map[string]interface{}{"status": "active"}},
			map[string]map[string]interface{}{"mailerLiteApi": {"apiKey": "ML"}},
			check{"POST", "/subscribers", []string{`"email":"a@b.c"`, `"status":"active"`, "Bearer ML"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == 5.0 }},
		{"coingecko price", "coinGecko", "coinGecko", 1, map[string]interface{}{"operation": "price", "coinIds": []interface{}{"bitcoin", "ethereum"}, "currencies": []interface{}{"usd"}},
			nil,
			check{"GET", "/simple/price", []string{"ids=bitcoin%2Cethereum", "vs_currencies=usd"}},
			func(o []map[string]interface{}) bool { return len(o) == 2 && asMap(o[0]["bitcoin"])["usd"] == 1.0 }},
		{"mailjet send", "mailjet", "mailjet", 1, map[string]interface{}{"fromEmail": "a@x.io", "toEmail": "b@x.io, c@x.io", "subject": "S", "text": "T", "additionalFields": map[string]interface{}{"fromName": "Ada"}},
			map[string]map[string]interface{}{"mailjetEmailApi": {"apiKey": "k", "secretKey": "s"}},
			check{"POST", "/v3.1/send", []string{`"To":[{"Email":"b@x.io"},{"Email":"c@x.io"}]`, `"Name":"Ada"`, "Basic azpz"}},
			func(o []map[string]interface{}) bool { return o[0]["Status"] == "success" }},
		{"freshdesk ticket", "freshdesk", "freshdesk", 1, map[string]interface{}{"requester": "email", "requesterIdentificationValue": "a@b.c", "options": map[string]interface{}{"subject": "Help", "description": "d", "tags": "a,b"}},
			map[string]map[string]interface{}{"freshdeskApi": {"domain": "acme", "apiKey": "F"}},
			check{"POST", "/tickets", []string{`"email":"a@b.c"`, `"status":3`, `"priority":1`, `"source":2`, `"tags":["a","b"]`}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == 11.0 }},
		{"microsoft to do", "microsoftToDo", "microsoftToDo", 1, map[string]interface{}{"taskListId": "L1", "title": "Buy", "additionalFields": map[string]interface{}{"dueDateTime": "2024-01-01T00:00:00Z", "importance": "high"}},
			map[string]map[string]interface{}{"microsoftToDoOAuth2Api": {"accessToken": "MS"}},
			check{"POST", "/lists/L1/tasks", []string{`"title":"Buy"`, `"timeZone":"Etc/GMT"`, `"importance":"high"`, "Bearer MS"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "t1" }},
		{"raindrop bookmark", "raindrop", "raindrop", 1, map[string]interface{}{"resource": "bookmark", "operation": "create", "link": "http://x", "collectionId": "5", "additionalFields": map[string]interface{}{"tags": "a,b"}},
			map[string]map[string]interface{}{"raindropOAuth2Api": {"accessToken": "R"}},
			check{"POST", "/raindrop", []string{`"link":"http://x"`, `"collection":{"$id":"5"}`, `"tags":["a","b"]`}},
			func(o []map[string]interface{}) bool { return o[0]["_id"] == 1.0 }},
		{"medium post", "medium", "medium", 1, map[string]interface{}{"title": "T", "contentFormat": "html", "content": "<p>x</p>", "additionalFields": map[string]interface{}{"tags": "go,n8n"}},
			map[string]map[string]interface{}{"mediumApi": {"accessToken": "MD"}},
			check{"POST", "/users/U1/posts", []string{`"title":"T"`, `"tags":["go","n8n"]`, "Bearer MD"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "p1" }},
		{"google books search", "googleBooks", "googleBooks", 2, map[string]interface{}{"authentication": "oAuth2", "resource": "volume", "operation": "getAll", "searchQuery": "go", "limit": 1},
			map[string]map[string]interface{}{"googleBooksOAuth2Api": {"accessToken": "GB"}},
			check{"GET", "/volumes", []string{"q=go", "Bearer GB"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "v1" }},
		{"google natural language", "googleCloudNaturalLanguage", "googleCloudNaturalLanguage", 1, map[string]interface{}{"content": "I love it"},
			map[string]map[string]interface{}{"googleCloudNaturalLanguageOAuth2Api": {"accessToken": "NL"}},
			check{"POST", "/documents:analyzeSentiment", []string{`"content":"I love it"`, `"type":"PLAIN_TEXT"`, `"encodingType":"UTF16"`}},
			func(o []map[string]interface{}) bool { return asMap(o[0]["documentSentiment"])["score"] == 0.8 }},
		{"intercom user", "intercom", "intercom", 1, map[string]interface{}{"identifierType": "email", "idValue": "a@b.c", "additionalFields": map[string]interface{}{"name": "Ada"}},
			map[string]map[string]interface{}{"intercomApi": {"apiKey": "IC"}},
			check{"POST", "/users", []string{`"email":"a@b.c"`, `"name":"Ada"`, "Bearer IC"}},
			func(o []map[string]interface{}) bool { return o[0]["id"] == "iu1" }},
		{"quickchart", "quickChart", "quickChart", 1, map[string]interface{}{"chartType": "line", "labelsMode": "array", "labelsArray": "Jan, Feb", "data": "[1, 2]", "datasetOptions": map[string]interface{}{"label": "Sales"}},
			nil,
			check{"POST", "/chart", []string{`"type":"line"`, `"labels":["Jan","Feb"]`, `"data":[1,2]`, `"label":"Sales"`, `"format":"png"`}},
			func(o []map[string]interface{}) bool { return o[0]["chart"] != nil }},
		{"google business profile post", "googleBusinessProfile", "googleBusinessProfile", 1, map[string]interface{}{"account": "accounts/1", "location": "locations/2", "summary": "Hi"},
			map[string]map[string]interface{}{"googleBusinessProfileOAuth2Api": {"accessToken": "GP"}},
			check{"POST", "/accounts/1/locations/2/localPosts", []string{`"summary":"Hi"`, `"topicType":"STANDARD"`}},
			func(o []map[string]interface{}) bool { return strings.HasSuffix(toString(o[0]["name"]), "/3") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			creds := c.creds
			if creds == nil {
				creds = map[string]map[string]interface{}{"none": {}}
			}
			out := restRunV(t, f, c.api, c.node, c.version, c.params, creds)
			if len(out) == 0 || !c.out(out) {
				t.Fatalf("output: %v", out)
			}
			r := f.findExact(t, c.req.method, c.req.path)
			all := r.query + " " + r.body + " " + r.auth
			for _, h := range c.req.has {
				if !strings.Contains(all, h) {
					t.Errorf("request lacks %q: %s %s?%s body=%s auth=%s", h, r.method, r.path, r.query, r.body, r.auth)
				}
			}
		})
	}
}

// findExact returns the last request with this method whose path equals
// path, or starts with it when path ends in "/".
func (f *fakeAPI) findExact(t *testing.T, method, path string) recorded {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := len(f.reqs) - 1; k >= 0; k-- {
		r := f.reqs[k]
		if r.method == method && (r.path == path || strings.HasSuffix(path, "/") && strings.HasPrefix(r.path, path)) {
			return r
		}
	}
	t.Fatalf("no %s request to %s", method, path)
	return recorded{}
}
