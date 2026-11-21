package n8n

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Service nodes: Clockify, PagerDuty, QuickChart, Webflow, Google Chat,
// Zammad, Mindee, Rocket.Chat, Bannerbear and UptimeRobot.

func init() {
	registerClockify()
	registerPagerDuty()
	registerWebflow()
	registerGoogleChat()
	registerZammad()
	registerMindee()
	registerRocketchat()
	registerBannerbear()
	registerUptimeRobot()
}

// ---------------------------------------------------------------- Clockify

func registerClockify() {
	w := "/workspaces/{workspaceId}"
	list := func(path, filters string) *restOp {
		return &restOp{path: path, list: true, pager: pageNumber("page", 1), sizeParam: "page-size", maxSize: 50, query: filters,
			build: func(_ *execCtx, _ int, r *restRequest) error { r.query.Set("page", "1"); return nil }}
	}
	registerREST("clockify", &restService{
		defResource: "project",
		defOps:      map[string]string{"project": "create", "client": "create", "tag": "create", "task": "create", "timeEntry": "create", "user": "getAll", "workspace": "getAll"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("clockifyApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("clockify", "https://api.clockify.me/api/v1"), headers: map[string]string{"X-Api-Key": credString(c, "apiKey")}}, nil
		},
		ops: map[string]*restOp{
			"project/create": {method: "POST", path: w + "/projects", body: "name @additionalFields billable clientId color isPublic note estimate.estimate:estimate.estimate estimate.type:estimate.type"},
			"project/get":    {path: w + "/projects/{projectId}"},
			"project/getAll": list(w+"/projects", "@additionalFields archived billable clients contains-client:contains-client client-status:client-status contains-user:contains-user is-template:is-template name sort-column:sort-column sort-order:sort-order users"),
			"project/update": {method: "PUT", path: w + "/projects/{projectId}", body: "@updateFields *"},
			"project/delete": {method: "DELETE", path: w + "/projects/{projectId}", success: true},
			"client/create":  {method: "POST", path: w + "/clients", body: "name"},
			"client/get":     {path: w + "/clients/{clientId}"},
			"client/getAll":  list(w+"/clients", "@additionalFields archived name sort-order:sort-order"),
			"client/update":  {method: "PUT", path: w + "/clients/{clientId}", body: "name @updateFields *"},
			"client/delete":  {method: "DELETE", path: w + "/clients/{clientId}"},
			"tag/create":     {method: "POST", path: w + "/tags", body: "name"},
			"tag/getAll":     list(w+"/tags", "@additionalFields archived name sort-column:sort-column sort-order:sort-order"),
			"tag/update":     {method: "PUT", path: w + "/tags/{tagId}", body: "@updateFields *"},
			"tag/delete":     {method: "DELETE", path: w + "/tags/{tagId}", success: true},
			"task/create":    {method: "POST", path: w + "/projects/{projectId}/tasks", body: "name @additionalFields assigneeIds estimate status"},
			"task/get":       {path: w + "/projects/{projectId}/tasks/{taskId}"},
			"task/getAll":    list(w+"/projects/{projectId}/tasks", "@filters is-active:is-active name sort-column:sort-column sort-order:sort-order"),
			"task/update":    {method: "PUT", path: w + "/projects/{projectId}/tasks/{taskId}", body: "@updateFields *"},
			"task/delete":    {method: "DELETE", path: w + "/projects/{projectId}/tasks/{taskId}", success: true},
			"timeEntry/create": {method: "POST", path: w + "/time-entries", body: "start @additionalFields billable description end projectId taskId tagIds", build: func(ec *execCtx, i int, r *restRequest) error {
				if v, ok := r.body["customFields"]; ok {
					r.body["customFields"] = asList(asMap(v)["customFieldsValues"])
				}
				return nil
			}},
			"timeEntry/get":    {path: w + "/time-entries/{timeEntryId}", query: "@additionalFields consider-duration-format:consider-duration-format hydrated"},
			"timeEntry/update": {method: "PUT", path: w + "/time-entries/{timeEntryId}", body: "@updateFields *"},
			"timeEntry/delete": {method: "DELETE", path: w + "/time-entries/{timeEntryId}", success: true},
			"user/getAll":      list(w+"/users", "@filters email name status sort-column:sort-column sort-order:sort-order"),
			"workspace/getAll": {path: "/workspaces", list: true},
		},
	})
}

// ---------------------------------------------------------------- PagerDuty

func registerPagerDuty() {
	from := func(ec *execCtx, i int, r *restRequest) error {
		if e, _ := ec.str(i, "email", ""); e != "" {
			r.headers = map[string]string{"From": e}
		}
		return nil
	}
	registerREST("pagerDuty", &restService{
		defResource: "incident",
		defOps:      map[string]string{"incident": "create", "incidentNote": "create", "logEntry": "get", "user": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, t, err := ec.restCred(i, "pagerDutyApi", map[string]string{"apiToken": "pagerDutyApi", "oAuth2": "pagerDutyOAuth2Api"})
			if err != nil {
				return nil, err
			}
			auth := "Token token=" + credString(c, "apiToken")
			if t == "pagerDutyOAuth2Api" {
				auth = "Bearer " + accessToken(c)
			}
			return &restClient{base: ec.apiBase("pagerDuty", "https://api.pagerduty.com"), headers: map[string]string{"Authorization": auth, "Accept": "application/vnd.pagerduty+json;version=2"}}, nil
		},
		ops: map[string]*restOp{
			"incident/create": {method: "POST", path: "/incidents", out: "incident", build: func(ec *execCtx, i int, r *restRequest) error {
				inc := map[string]interface{}{"type": "incident"}
				title, _ := ec.str(i, "title", "")
				svc, _ := ec.str(i, "serviceId", "")
				inc["title"] = title
				inc["service"] = map[string]interface{}{"id": svc, "type": "service_reference"}
				f := fieldsOf(ec, i, "additionalFields")
				pdIncidentFields(inc, f)
				if cb := asMap(asMap(ec.mustParam(i, "conferenceBridgeUi"))["conferenceBridgeValues"]); len(cb) > 0 {
					inc["conference_bridge"] = map[string]interface{}{"conference_number": cb["conferenceNumber"], "conference_url": cb["conferenceUrl"]}
				}
				r.body["incident"] = inc
				return from(ec, i, r)
			}},
			"incident/get":    {path: "/incidents/{incidentId}", out: "incident"},
			"incident/getAll": {path: "/incidents", list: true, listKey: "incidents", pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100, query: "@options dateRange:date_range incidentKey:incident_key include:include[] serviceIds:service_ids[] since sortBy:sort_by statuses:statuses[] teamIds:team_ids[] timeZone:time_zone until urgencies:urgencies[] userIds:user_ids[]"},
			"incident/update": {method: "PUT", path: "/incidents/{incidentId}", out: "incident", build: func(ec *execCtx, i int, r *restRequest) error {
				inc := map[string]interface{}{"type": "incident"}
				pdIncidentFields(inc, fieldsOf(ec, i, "updateFields"))
				if cb := asMap(asMap(ec.mustParam(i, "conferenceBridgeUi"))["conferenceBridgeValues"]); len(cb) > 0 {
					inc["conference_bridge"] = map[string]interface{}{"conference_number": cb["conferenceNumber"], "conference_url": cb["conferenceUrl"]}
				}
				r.body["incident"] = inc
				return from(ec, i, r)
			}},
			"incidentNote/create": {method: "POST", path: "/incidents/{incidentId}/notes", body: "content:note.content", out: "note", build: from},
			"incidentNote/getAll": {path: "/incidents/{incidentId}/notes", list: true, listKey: "notes"},
			"logEntry/get":        {path: "/log_entries/{logEntryId}", out: "log_entry"},
			"logEntry/getAll":     {path: "/log_entries", list: true, listKey: "log_entries", pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100, query: "@options include:include[] isOverview:is_overview since timeZone:time_zone until"},
			"user/get":            {path: "/users/{userId}", out: "user"},
		},
	})
}

func pdIncidentFields(inc, f map[string]interface{}) {
	for k, v := range f {
		if restEmpty(v) {
			continue
		}
		switch k {
		case "details":
			inc["body"] = map[string]interface{}{"type": "incident_body", "details": v}
		case "priorityId":
			inc["priority"] = map[string]interface{}{"id": v, "type": "priority_reference"}
		case "escalationPolicyId":
			inc["escalation_policy"] = map[string]interface{}{"id": v, "type": "escalation_policy_reference"}
		case "incidentKey":
			inc["incident_key"] = v
		case "escalationLevel":
			inc["escalation_level"] = v
		default:
			inc[snakeCase(k)] = v
		}
	}
}

// ---------------------------------------------------------------- QuickChart

// quickChartNode renders one chart from all input items: each item is a
// dataset, labels come from the first item.
func quickChartNode(ec *execCtx) ([][]Item, error) {
	items := ec.items()
	if len(items) == 0 {
		return [][]Item{nil}, nil
	}
	typ, _ := ec.str(0, "chartType", "bar")
	var labels []interface{}
	if mode, _ := ec.str(0, "labelsMode", "manually"); mode == "array" {
		v, _ := ec.param(0, "labelsArray", nil)
		if s, ok := v.(string); ok {
			x, err := restConv("json", s)
			if err != nil {
				for _, p := range strings.Split(s, ",") {
					labels = append(labels, strings.TrimSpace(p))
				}
			} else {
				labels = asList(x)
			}
		} else {
			labels = asList(v)
		}
	} else {
		v, _ := ec.param(0, "labelsUi", nil)
		for _, r := range asList(asMap(v)["labelsValues"]) {
			l := asMap(r)["label"]
			if s, ok := l.(string); ok && strings.Contains(s, ",") {
				for _, p := range strings.Split(s, ",") {
					labels = append(labels, strings.TrimSpace(p))
				}
				continue
			}
			labels = append(labels, l)
		}
	}
	var datasets []interface{}
	for i := range items {
		v, _ := ec.param(i, "data", "[]")
		data := v
		if s, ok := v.(string); ok {
			x, err := restConv("json", s)
			if err != nil {
				return nil, permanentError{"QuickChart: Data must be a JSON array"}
			}
			data = x
		}
		ds := map[string]interface{}{"data": data}
		for k, x := range fieldsOf(ec, i, "datasetOptions") {
			if !restEmpty(x) {
				ds[k] = x
			}
		}
		datasets = append(datasets, ds)
	}
	o := fieldsOf(ec, 0, "chartOptions")
	chartType := typ
	if typ == "bar" && toBool(o["horizontal"]) {
		chartType = "horizontalBar"
	}
	req := map[string]interface{}{
		"chart":            map[string]interface{}{"type": chartType, "data": map[string]interface{}{"labels": labels, "datasets": datasets}},
		"width":            firstSet(o["width"], 500),
		"height":           firstSet(o["height"], 300),
		"backgroundColor":  firstSet(o["backgroundColor"], "white"),
		"devicePixelRatio": firstSet(o["devicePixelRatio"], 2),
		"format":           firstNonEmpty(toString(o["format"]), "png"),
	}
	res, err := ec.doRequest("POST", ec.apiBase("quickChart", "https://quickchart.io")+"/chart", map[string]string{}, []byte(mustJSON(req)), "application/json")
	if err != nil {
		return nil, err
	}
	if res.status >= 400 {
		return nil, fmt.Errorf("QuickChart: %d %s", res.status, strings.TrimSpace(string(res.body)))
	}
	format := toString(req["format"])
	mt := map[string]string{"png": "image/png", "svg": "image/svg+xml", "webp": "image/webp", "pdf": "application/pdf"}[format]
	prop, _ := ec.str(0, "output", "data")
	paired := make([]interface{}, len(items))
	for i := range items {
		paired[i] = map[string]interface{}{"item": i}
	}
	return [][]Item{{{JSON: map[string]interface{}{"chart": req["chart"]}, Binary: map[string]interface{}{prop: binaryData(res.body, firstNonEmpty(mt, res.contentType), "chart."+format)}, PairedItem: paired}}}, nil
}

// ---------------------------------------------------------------- Webflow

func registerWebflow() {
	registerREST("webflow", &restService{
		defResource: "item",
		defOps:      map[string]string{"item": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, _, err := ec.restCred(i, "webflowApi", map[string]string{"accessToken": "webflowApi", "oAuth2": "webflowOAuth2Api"})
			if err != nil {
				return nil, err
			}
			h := map[string]string{"Authorization": "Bearer " + firstNonEmpty(credString(c, "accessToken"), accessToken(c))}
			if ec.version < 2 {
				h["accept-version"] = "1.0.0"
				return &restClient{base: ec.apiBase("webflow", "https://api.webflow.com"), headers: h}, nil
			}
			return &restClient{base: ec.apiBase("webflow", "https://api.webflow.com/v2"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"item/create": {method: "POST", path: "/collections/{collectionId}/items", build: webflowBody},
			"item/update": {method: "PATCH", path: "/collections/{collectionId}/items/{itemId}", build: webflowBody},
			"item/get":    {path: "/collections/{collectionId}/items/{itemId}", post: webflowFirst},
			"item/delete": {method: "DELETE", path: "/collections/{collectionId}/items/{itemId}", success: true},
			"item/getAll": {path: "/collections/{collectionId}/items", list: true, listKey: "items", pager: offsetPager("offset"), sizeParam: "limit", maxSize: 100},
		},
	})
}

func webflowBody(ec *execCtx, i int, r *restRequest) error {
	live, _ := ec.boolean(i, "live", false)
	fields := map[string]interface{}{}
	if js, _ := ec.boolean(i, "jsonParameters", false); js {
		v, _ := ec.param(i, "fieldsJson", "{}")
		x, _ := restConv("json", v)
		fields = asMap(x)
	} else {
		v, _ := ec.param(i, "fieldsUi", nil)
		for k, x := range rowsToMap(asMap(v)["fieldValues"], "fieldId", "fieldValue") {
			fields[k] = x
		}
	}
	if ec.version < 2 {
		fields["_archived"] = false
		fields["_draft"] = false
		r.body["fields"] = fields
		r.query.Set("live", strconv.FormatBool(live))
		return nil
	}
	r.body["isArchived"] = false
	r.body["isDraft"] = false
	r.body["fieldData"] = fields
	if live {
		r.path += "/live"
	}
	return nil
}

// webflowFirst unwraps API v1's {items: [item]} responses.
func webflowFirst(ec *execCtx, _ int, v interface{}) (interface{}, error) {
	if l := asList(asMap(v)["items"]); ec.version < 2 && len(l) > 0 {
		return l[0], nil
	}
	return v, nil
}

// ---------------------------------------------------------------- Google Chat

func registerGoogleChat() {
	registerREST("googleChat", &restService{
		defResource: "message",
		defOps:      map[string]string{"message": "create", "space": "getAll", "member": "getAll", "attachment": "get", "incomingWebhook": "create", "media": "download"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			h, err := googleAuthDef(ec, i, "https://www.googleapis.com/auth/chat.bot", "googleChatOAuth2Api", "serviceAccount")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("googleChat", "https://chat.googleapis.com/v1"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"message/create": {method: "POST", path: "/{+spaceId}/messages", query: "@additionalFields requestId threadKey messageReplyOption", build: gchatMessage("messageUi", "messageJson")},
			"message/get":    {path: "/{+messageId}"},
			"message/delete": {method: "DELETE", path: "/{+messageId}", success: true},
			"message/update": {method: "PUT", path: "/{+messageId}", build: func(ec *execCtx, i int, r *restRequest) error {
				if err := gchatMessage("updateFieldsUi", "updateFieldsJson")(ec, i, r); err != nil {
					return err
				}
				var mask []string
				for _, k := range []string{"text", "cards"} {
					if _, ok := r.body[k]; ok {
						mask = append(mask, k)
					}
				}
				r.query.Set("updateMask", strings.Join(mask, ","))
				return nil
			}},
			"space/get":      {path: "/{+spaceId}"},
			"space/getAll":   {path: "/spaces", list: true, listKey: "spaces", pager: cursorPager("nextPageToken", "pageToken"), sizeParam: "pageSize", maxSize: 1000},
			"member/get":     {path: "/{+memberId}"},
			"member/getAll":  {path: "/{+spaceId}/members", list: true, listKey: "memberships", pager: cursorPager("nextPageToken", "pageToken"), sizeParam: "pageSize", maxSize: 1000},
			"attachment/get": {path: "/{+attachmentName}"},
			"incomingWebhook/create": {run: func(ec *execCtx, i int, _ Item, _ *restClient) ([]Item, error) {
				u, _ := ec.str(i, "incomingWebhookUrl", "")
				body := map[string]interface{}{}
				r := &restRequest{body: body, query: url.Values{}}
				if err := gchatMessage("messageUi", "messageJson")(ec, i, r); err != nil {
					return nil, err
				}
				if tk := toString(fieldsOf(ec, i, "additionalFields")["threadKey"]); tk != "" {
					u += map[bool]string{true: "&", false: "?"}[strings.Contains(u, "?")] + "threadKey=" + url.QueryEscape(tk)
				}
				res, err := ec.doRequest("POST", u, map[string]string{}, []byte(mustJSON(r.body)), "application/json")
				if err != nil {
					return nil, err
				}
				if res.status >= 400 {
					return nil, fmt.Errorf("google chat: %d %s", res.status, strings.TrimSpace(string(res.body)))
				}
				return restItems(decodeJSONBody(res.body)), nil
			}},
		},
	})
}

func gchatMessage(ui, js string) func(ec *execCtx, i int, r *restRequest) error {
	return func(ec *execCtx, i int, r *restRequest) error {
		if j, _ := ec.boolean(i, "jsonParameters", false); j {
			v, _ := ec.param(i, js, "{}")
			x, err := restConv("json", v)
			if err != nil {
				return permanentError{"Google Chat: message JSON is invalid"}
			}
			for k, val := range asMap(x) {
				r.body[k] = val
			}
			return nil
		}
		m := asMap(ec.mustParam(i, ui))
		if t := toString(m["text"]); t != "" {
			r.body["text"] = t
		}
		return nil
	}
}

// ---------------------------------------------------------------- Zammad

func registerZammad() {
	custom := func(coll string) func(ec *execCtx, i int, r *restRequest) error {
		return func(ec *execCtx, i int, r *restRequest) error {
			if v, ok := r.body["customFieldsUi"]; ok {
				delete(r.body, "customFieldsUi")
				for k, x := range rowsToMap(asMap(v)["customFieldPairs"], "name", "value") {
					r.body[k] = x
				}
			}
			if v, ok := r.body["addressUi"]; ok {
				delete(r.body, "addressUi")
				for k, x := range asMap(asMap(v)["addressDetails"]) {
					r.body[k] = x
				}
			}
			return nil
		}
	}
	search := func(path string) *restOp {
		return &restOp{path: path, list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 100, build: func(ec *execCtx, i int, r *restRequest) error {
			r.query.Set("page", "1")
			q := toString(fieldsOf(ec, i, "filters")["query"])
			if q != "" {
				r.path += "/search"
				r.query.Set("query", q)
			}
			if s := toString(fieldsOf(ec, i, "filters")["sortBy"]); s != "" {
				r.query.Set("sort_by", s)
			}
			return nil
		}}
	}
	registerREST("zammad", &restService{
		defResource: "user",
		defOps:      map[string]string{"user": "create", "organization": "create", "group": "create", "ticket": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, t, err := ec.restCred(i, "zammadTokenAuthApi", map[string]string{"tokenAuth": "zammadTokenAuthApi", "basicAuth": "zammadBasicAuthApi"})
			if err != nil {
				return nil, err
			}
			base := ec.apiBase("zammad", strings.TrimRight(credString(c, "baseUrl", "url"), "/")) + "/api/v1"
			auth := "Token token=" + credString(c, "accessToken")
			if t == "zammadBasicAuthApi" {
				auth = basicHeader(credString(c, "username", "user"), credString(c, "password"))
			}
			return &restClient{base: base, headers: map[string]string{"Authorization": auth}}, nil
		},
		ops: map[string]*restOp{
			"user/create":         {method: "POST", path: "/users", body: "firstname lastname @additionalFields *", build: custom("additionalFields")},
			"user/update":         {method: "PUT", path: "/users/{id}", body: "@updateFields *", build: custom("updateFields")},
			"user/get":            {path: "/users/{id}"},
			"user/getSelf":        {path: "/users/me"},
			"user/delete":         {method: "DELETE", path: "/users/{id}", success: true},
			"user/getAll":         search("/users"),
			"organization/create": {method: "POST", path: "/organizations", body: "name @additionalFields *", build: custom("additionalFields")},
			"organization/update": {method: "PUT", path: "/organizations/{id}", body: "@updateFields *", build: custom("updateFields")},
			"organization/get":    {path: "/organizations/{id}"},
			"organization/delete": {method: "DELETE", path: "/organizations/{id}", success: true},
			"organization/getAll": search("/organizations"),
			"group/create":        {method: "POST", path: "/groups", body: "name @additionalFields *", build: custom("additionalFields")},
			"group/update":        {method: "PUT", path: "/groups/{id}", body: "@updateFields *", build: custom("updateFields")},
			"group/get":           {path: "/groups/{id}"},
			"group/delete":        {method: "DELETE", path: "/groups/{id}", success: true},
			"group/getAll":        {path: "/groups", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 100},
			"ticket/create": {method: "POST", path: "/tickets", body: "title group customer", build: func(ec *execCtx, i int, r *restRequest) error {
				a := asMap(asMap(ec.mustParam(i, "article"))["articleDetails"])
				r.body["article"] = map[string]interface{}{"subject": a["subject"], "body": a["body"], "type": firstNonEmpty(toString(a["type"]), "note"), "internal": toBool(a["visibility"] == "internal")}
				return custom("additionalFields")(ec, i, r)
			}},
			"ticket/get":    {path: "/tickets/{id}", query: "=expand:true"},
			"ticket/delete": {method: "DELETE", path: "/tickets/{id}", success: true},
			"ticket/getAll": {path: "/tickets", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 100, query: "=expand:true"},
		},
	})
}

// ---------------------------------------------------------------- Mindee

func registerMindee() {
	predict := func(product string) *restOp {
		return &restOp{run: func(ec *execCtx, i int, it Item, c *restClient) ([]Item, error) {
			prop, _ := ec.str(i, "binaryPropertyName", "data")
			data, mt, name, err := binaryOf(it, prop)
			if err != nil {
				return nil, err
			}
			body, ct := cmtyMultipart(nil, cmtyFile{field: "document", name: firstNonEmpty(name, "document"), mimeType: mt, data: data})
			res, err := cmtyRaw(ec, c, "POST", "/products/mindee/"+product+"/predict", nil, body, ct)
			if err != nil {
				return nil, err
			}
			v := decodeJSONBody(res.body)
			if res.status >= 400 {
				return nil, fmt.Errorf("mindee: %d %s", res.status, mustJSON(v))
			}
			if raw, _ := ec.boolean(i, "rawData", false); raw {
				return restItems(v), nil
			}
			return []Item{{JSON: mindeeSimplify(jpath(v, "document.inference.prediction"))}}, nil
		}}
	}
	registerREST("mindee", &restService{
		defResource: "receipt",
		defOps:      map[string]string{"receipt": "predict", "invoice": "predict"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			r, _ := ec.str(i, "resource", "receipt")
			c, err := ec.credential(map[string]string{"receipt": "mindeeReceiptApi", "invoice": "mindeeInvoiceApi"}[r])
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("mindee", "https://api.mindee.net/v1"), headers: map[string]string{"Authorization": "Token " + credString(c, "apiKey")}}, nil
		},
		ops: map[string]*restOp{
			"receipt/predict": predict("expense_receipts/v5"),
			"invoice/predict": predict("invoices/v4"),
		},
	})
}

// mindeeSimplify keeps each prediction field's value.
func mindeeSimplify(v interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	val := func(x interface{}) interface{} {
		m := asMap(x)
		if v, ok := m["value"]; ok {
			return v
		}
		if v, ok := m["amount"]; ok {
			return v
		}
		return x
	}
	for k, x := range asMap(v) {
		if l, ok := x.([]interface{}); ok {
			var vs []interface{}
			for _, e := range l {
				vs = append(vs, val(e))
			}
			out[k] = vs
			continue
		}
		out[k] = val(x)
	}
	return out
}

// ---------------------------------------------------------------- Rocket.Chat

func registerRocketchat() {
	registerREST("rocketchat", &restService{
		defResource: "chat",
		defOps:      map[string]string{"chat": "postMessage"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("rocketchatApi")
			if err != nil {
				return nil, err
			}
			base := ec.apiBase("rocketchat", strings.TrimRight(credString(c, "domain", "url"), "/")) + "/api/v1"
			return &restClient{base: base, headers: map[string]string{"X-Auth-Token": credString(c, "authKey"), "X-User-Id": credString(c, "userId")}}, nil
		},
		ops: map[string]*restOp{
			"chat/postMessage": {method: "POST", path: "/chat.postMessage", body: "channel text @options alias avatar emoji", build: func(ec *execCtx, i int, r *restRequest) error {
				if js, _ := ec.boolean(i, "jsonParameters", false); js {
					v, _ := ec.param(i, "attachmentsJson", "[]")
					x, err := restConv("json", v)
					if err != nil {
						return permanentError{"Rocketchat: Attachments JSON is invalid"}
					}
					r.body["attachments"] = x
					return nil
				}
				var atts []interface{}
				for _, a := range asList(ec.mustParam(i, "attachments")) {
					m := map[string]interface{}{}
					for k, v := range asMap(a) {
						if restEmpty(v) {
							continue
						}
						if k == "fields" {
							m["fields"] = asList(asMap(v)["fieldsValues"])
							continue
						}
						m[snakeCase(k)] = v
					}
					atts = append(atts, m)
				}
				if atts != nil {
					r.body["attachments"] = atts
				}
				return nil
			}},
		},
	})
}

// ---------------------------------------------------------------- Bannerbear

func registerBannerbear() {
	registerREST("bannerbear", &restService{
		defResource: "image",
		defOps:      map[string]string{"image": "create", "template": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("bannerbearApi")
			if err != nil {
				return nil, err
			}
			return bearerClient(ec.apiBase("bannerbear", "https://api.bannerbear.com/v2"), c, "apiKey"), nil
		},
		ops: map[string]*restOp{
			"image/create":    {run: bannerbearCreate},
			"image/get":       {path: "/images/{imageId}"},
			"template/get":    {path: "/templates/{templateId}"},
			"template/getAll": {path: "/templates", list: true},
		},
	})
}

func bannerbearCreate(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
	tpl, _ := ec.str(i, "templateId", "")
	body := map[string]interface{}{"template": tpl}
	var mods []interface{}
	for _, m := range asList(asMap(ec.mustParam(i, "modificationsUi"))["modificationsValues"]) {
		out := map[string]interface{}{}
		for k, v := range asMap(m) {
			if !restEmpty(v) {
				out[snakeCase(k)] = v
			}
		}
		mods = append(mods, out)
	}
	body["modifications"] = mods
	f := fieldsOf(ec, i, "additionalFields")
	for _, k := range []string{"webhookUrl", "metadata"} {
		if v := f[k]; !restEmpty(v) {
			body[snakeCase(k)] = v
		}
	}
	v, err := c.do(ec, &restRequest{method: "POST", path: "/images", query: url.Values{}, body: body})
	if err != nil {
		return nil, err
	}
	if toBool(f["waitForImage"]) {
		tries := int(toFloatDefault(f["waitForImageMaxTries"], 3))
		uid := toString(asMap(v)["uid"])
		for n := 0; n < tries && toString(asMap(v)["status"]) != "completed"; n++ {
			if err := cmtySleep(ec, 2*time.Second); err != nil {
				return nil, err
			}
			if v, err = c.do(ec, &restRequest{method: "GET", path: "/images/" + url.PathEscape(uid), query: url.Values{}}); err != nil {
				return nil, err
			}
		}
	}
	return restItems(v), nil
}

// ---------------------------------------------------------------- UptimeRobot

func registerUptimeRobot() {
	key := func(ec *execCtx, _ int, r *restRequest) error {
		c, err := ec.credential("uptimeRobotApi")
		if err != nil {
			return err
		}
		r.body["api_key"] = credString(c, "apiKey")
		r.body["format"] = "json"
		return nil
	}
	op := func(path, body, out string) *restOp {
		return &restOp{method: "POST", path: path, body: body, form: true, out: out, build: key}
	}
	registerREST("uptimeRobot", &restService{
		defResource: "account",
		defOps:      map[string]string{"account": "get", "monitor": "create", "alertContact": "create", "maintenanceWindow": "create", "publicStatusPage": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("uptimeRobotApi")
			if err != nil {
				return nil, err
			}
			_ = c
			return &restClient{base: ec.apiBase("uptimeRobot", "https://api.uptimerobot.com/v2")}, nil
		},
		ops: map[string]*restOp{
			"account/get":         op("/getAccountDetails", "", "account"),
			"monitor/create":      op("/newMonitor", "friendlyName:friendly_name url type @additionalFields *snake", "monitor"),
			"monitor/get":         op("/getMonitors", "id:monitors", "monitors.0"),
			"monitor/getAll":      {method: "POST", path: "/getMonitors", form: true, list: true, listKey: "monitors", pager: offsetPager("offset"), sizeParam: "limit", maxSize: 50, query: "@filters *snake", build: key},
			"monitor/update":      op("/editMonitor", "id @updateFields *snake", "monitor"),
			"monitor/delete":      op("/deleteMonitor", "id", "monitor"),
			"monitor/reset":       op("/resetMonitor", "id", "monitor"),
			"alertContact/create": op("/newAlertContact", "friendlyName:friendly_name type value", "alertcontact"),
			"alertContact/get":    op("/getAlertContacts", "id:alert_contacts", "alert_contacts.0"),
			"alertContact/getAll": {method: "POST", path: "/getAlertContacts", form: true, list: true, listKey: "alert_contacts", pager: offsetPager("offset"), sizeParam: "limit", maxSize: 50, query: "@filters *snake", build: key},
			"alertContact/update": op("/editAlertContact", "id @updateFields *snake", "alert_contact"),
			"alertContact/delete": op("/deleteAlertContact", "id", "alert_contact"),
		},
	})
}
