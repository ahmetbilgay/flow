package n8n

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Service nodes: Strava, Matrix, sms77, MailerLite, CoinGecko, Mailjet,
// Freshdesk, Microsoft To Do, Raindrop, Medium, Google Books, Google Cloud
// Natural Language and Intercom.

func init() {
	registerStrava()
	registerMatrix()
	registerSms77()
	registerMailerLite()
	registerCoinGecko()
	registerMailjet()
	registerFreshdesk()
	registerMicrosoftToDo()
	registerRaindrop()
	registerMedium()
	registerGoogleBooks()
	registerGoogleNaturalLanguage()
	registerIntercom()
}

// ---------------------------------------------------------------- Strava

func registerStrava() {
	registerREST("strava", &restService{
		defResource: "activity",
		defOps:      map[string]string{"activity": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("stravaOAuth2Api")
			if err != nil {
				return nil, err
			}
			return bearerClient(ec.apiBase("strava", "https://www.strava.com/api/v3"), c), nil
		},
		ops: map[string]*restOp{
			"activity/create": {method: "POST", path: "/activities", body: "name type:sport_type startDate:start_date_local elapsedTime:elapsed_time @additionalFields description distance trainer:trainer:int commute:commute:int",
				build: func(ec *execCtx, i int, r *restRequest) error {
					if ec.version < 1.1 {
						r.body["type"] = r.body["sport_type"]
						delete(r.body, "sport_type")
					}
					return nil
				}},
			"activity/get":         {path: "/activities/{activityId}"},
			"activity/update":      {method: "PUT", path: "/activities/{activityId}", body: "@updateFields *snake"},
			"activity/getAll":      {path: "/athlete/activities", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 30, build: func(_ *execCtx, _ int, r *restRequest) error { r.query.Set("page", "1"); return nil }},
			"activity/getComments": {path: "/activities/{activityId}/comments", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 30},
			"activity/getKudos":    {path: "/activities/{activityId}/kudos", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 30},
			"activity/getLaps":     {path: "/activities/{activityId}/laps", list: true},
			"activity/getZones":    {path: "/activities/{activityId}/zones", list: true},
			"activity/getStreams":  {path: "/activities/{activityId}/streams", query: "keys:keys:csv =key_by_type:true"},
		},
	})
}

// ---------------------------------------------------------------- Matrix

func registerMatrix() {
	registerREST("matrix", &restService{
		defResource: "message",
		defOps:      map[string]string{"message": "create", "room": "create", "roomMember": "getAll", "account": "me", "event": "get", "media": "upload"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("matrixApi")
			if err != nil {
				return nil, err
			}
			base := ec.apiBase("matrix", strings.TrimRight(firstNonEmpty(credString(c, "homeserverUrl"), "https://matrix-client.matrix.org"), "/")) + "/_matrix/client/r0"
			return &restClient{base: base, headers: map[string]string{"Authorization": "Bearer " + credString(c, "accessToken")}}, nil
		},
		ops: map[string]*restOp{
			"message/create": {method: "PUT", path: "/rooms/{roomId}/send/m.room.message/{txn?}", build: func(ec *execCtx, i int, r *restRequest) error {
				text, _ := ec.str(i, "text", "")
				typ, _ := ec.str(i, "messageType", "m.text")
				r.body["msgtype"] = typ
				r.body["body"] = text
				if f, _ := ec.str(i, "messageFormat", "plain"); f == "org.matrix.custom.html" {
					r.body["format"] = f
					r.body["formatted_body"] = text
					if fb := toString(fieldsOf(ec, i, "additionalFields")["fallbackText"]); fb != "" {
						r.body["body"] = fb
					}
				}
				r.path += strconv.FormatInt(time.Now().UnixNano(), 36)
				return nil
			}},
			"message/getAll":    {path: "/rooms/{roomId}/messages", list: true, listKey: "chunk", pager: cursorPager("end", "from"), query: "=dir:b @otherOptions filter", sizeParam: "limit", maxSize: 100},
			"event/get":         {path: "/rooms/{roomId}/event/{eventId}"},
			"account/me":        {path: "/account/whoami"},
			"room/create":       {method: "POST", path: "/createRoom", body: "roomName:name preset roomAlias:room_alias_name"},
			"room/join":         {method: "POST", path: "/rooms/{roomIdOrAlias}/join"},
			"room/leave":        {method: "POST", path: "/rooms/{roomId}/leave", success: true},
			"room/invite":       {method: "POST", path: "/rooms/{roomId}/invite", body: "userId:user_id", success: true},
			"room/kick":         {method: "POST", path: "/rooms/{roomId}/kick", body: "userId:user_id reason", success: true},
			"roomMember/getAll": {path: "/rooms/{roomId}/members", query: "@filters notMembership:not_membership membership", out: "chunk"},
		},
	})
}

// ---------------------------------------------------------------- sms77 (seven)

func registerSms77() {
	registerREST("sms77", &restService{
		defResource: "sms",
		defOps:      map[string]string{"sms": "send", "voice": "send"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("sms77Api")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("sms77", "https://gateway.sms77.io/api"), headers: map[string]string{"X-Api-Key": credString(c, "apiKey")}}, nil
		},
		ops: map[string]*restOp{
			"sms/send": {method: "POST", path: "/sms", body: "from to message:text @options delay foreign_id flash label no_reload performance_tracking ttl", build: func(_ *execCtx, _ int, r *restRequest) error {
				r.body["json"] = 1
				return nil
			}},
			"voice/send": {method: "POST", path: "/voice", body: "from to message:text", build: func(_ *execCtx, _ int, r *restRequest) error {
				r.body["json"] = 1
				return nil
			}},
		},
	})
}

// ---------------------------------------------------------------- MailerLite

func registerMailerLite() {
	registerREST("mailerLite", &restService{
		defResource: "subscriber",
		defOps:      map[string]string{"subscriber": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("mailerLiteApi")
			if err != nil {
				return nil, err
			}
			if ec.version >= 2 {
				return bearerClient(ec.apiBase("mailerLite", "https://connect.mailerlite.com/api"), c, "apiKey"), nil
			}
			return &restClient{base: ec.apiBase("mailerLite", "https://api.mailerlite.com/api/v2"), headers: map[string]string{"X-MailerLite-ApiKey": credString(c, "apiKey")}}, nil
		},
		ops: map[string]*restOp{
			"subscriber/create": {method: "POST", path: "/subscribers", body: "email", build: mlFields("additionalFields")},
			"subscriber/update": {method: "PUT", path: "/subscribers/{subscriberId}", build: mlFields("updateFields")},
			"subscriber/get":    {path: "/subscribers/{subscriberId}", post: mlData},
			"subscriber/getAll": {path: "/subscribers", list: true, query: "@filters status:filter[status] type", build: func(ec *execCtx, i int, r *restRequest) error {
				if ec.version < 2 {
					if s := r.query.Get("filter[status]"); s != "" {
						r.query.Del("filter[status]")
						r.query.Set("type", s)
					}
				}
				return nil
			}, listKey: "data", pager: cursorPager("meta.next_cursor", "cursor"), sizeParam: "limit", maxSize: 100},
		},
	})
}

// mlFields maps custom fields and the remaining options of a subscriber.
func mlFields(coll string) func(ec *execCtx, i int, r *restRequest) error {
	return func(ec *execCtx, i int, r *restRequest) error {
		f := fieldsOf(ec, i, coll)
		for k, v := range f {
			if restEmpty(v) {
				continue
			}
			switch k {
			case "customFieldsUi":
				fields := rowsToMap(asMap(v)["customFieldsValues"], "fieldId", "value")
				if len(fields) > 0 {
					r.body["fields"] = fields
				}
			case "groups":
				r.body["groups"] = asListOrOne(v)
			default:
				if ec.version < 2 {
					r.body[k] = v
				} else {
					r.body[snakeCase(k)] = v
				}
			}
		}
		return nil
	}
}

func mlData(ec *execCtx, _ int, v interface{}) (interface{}, error) {
	if d, ok := asMap(v)["data"]; ok && ec.version >= 2 {
		return d, nil
	}
	return v, nil
}

// ---------------------------------------------------------------- CoinGecko

func registerCoinGecko() {
	registerREST("coinGecko", &restService{
		defResource: "coin",
		defOps:      map[string]string{"coin": "get", "event": "getAll"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			return &restClient{base: ec.apiBase("coinGecko", "https://api.coingecko.com/api/v3")}, nil
		},
		ops: map[string]*restOp{
			"coin/get": {path: "/coins/{coinId}", build: func(ec *execCtx, i int, r *restRequest) error {
				for _, k := range []string{"community_data", "developer_data", "localization", "market_data", "sparkline", "tickers"} {
					r.query.Set(k, "false")
				}
				for k, v := range fieldsOf(ec, i, "options") {
					r.query.Set(k, toString(v))
				}
				return nil
			}},
			"coin/getAll": {path: "/coins/list", list: true},
			"coin/market": {path: "/coins/markets", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 250,
				query: "baseCurrency:vs_currency @options ids:ids:csv category order sparkline price_change_percentage:price_change_percentage:csv",
				build: func(_ *execCtx, _ int, r *restRequest) error { r.query.Set("page", "1"); return nil }},
			"coin/price": {path: "/simple/price", query: "coinIds:ids:csv currencies:vs_currencies:csv @options include_24hr_change include_24hr_vol include_last_updated_at include_market_cap",
				build: func(ec *execCtx, i int, r *restRequest) error {
					if by, _ := ec.str(i, "searchBy", "coinId"); by == "contractAddress" {
						p, _ := ec.str(i, "platformId", "ethereum")
						r.path = "/simple/token_price/" + url.PathEscape(p)
						ca, _ := ec.str(i, "contractAddresses", "")
						r.query.Set("contract_addresses", ca)
						r.query.Set("vs_currencies", r.query.Get("vs_currencies"))
						r.query.Del("ids")
					}
					for _, k := range []string{"ids", "vs_currencies"} {
						if vs := r.query[k]; len(vs) > 0 {
							r.query.Set(k, strings.ReplaceAll(strings.Join(vs, ","), " ", ""))
						}
					}
					return nil
				},
				post: func(_ *execCtx, _ int, v interface{}) (interface{}, error) {
					m := asMap(v)
					keys := make([]string, 0, len(m))
					for k := range m {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					var out []interface{}
					for _, k := range keys {
						out = append(out, map[string]interface{}{k: m[k]})
					}
					return out, nil
				}},
			"coin/history": {path: "/coins/{coinId}/history", query: "date @options localization", build: func(_ *execCtx, _ int, r *restRequest) error {
				if t, err := time.Parse(time.RFC3339, r.query.Get("date")); err == nil {
					r.query.Set("date", t.Format("02-01-2006"))
				} else if t, err := time.Parse("2006-01-02", r.query.Get("date")[:min(10, len(r.query.Get("date")))]); err == nil {
					r.query.Set("date", t.Format("02-01-2006"))
				}
				return nil
			}},
			"coin/marketChart": {path: "/coins/{coinId}/market_chart", query: "baseCurrency:vs_currency days", post: func(_ *execCtx, _ int, v interface{}) (interface{}, error) {
				m := asMap(v)
				var out []interface{}
				for idx, p := range asList(m["prices"]) {
					row := map[string]interface{}{"time": time.UnixMilli(int64(toFloatDefault(firstElem(p), 0))).UTC().Format(time.RFC3339), "price": asList(p)[1]}
					if mc := asList(m["market_caps"]); idx < len(mc) {
						row["marketCaps"] = asList(mc[idx])[1]
					}
					if tv := asList(m["total_volumes"]); idx < len(tv) {
						row["totalVolumes"] = asList(tv[idx])[1]
					}
					out = append(out, row)
				}
				return out, nil
			}},
			"coin/ticker": {path: "/coins/{coinId}/tickers", list: true, listKey: "tickers", pager: pageNumber("page", 1), query: "@options exchange_ids:exchange_ids:csv include_exchange_logo order"},
			"coin/candlestick": {path: "/coins/{coinId}/ohlc", query: "baseCurrency:vs_currency days", post: func(_ *execCtx, _ int, v interface{}) (interface{}, error) {
				var out []interface{}
				for _, p := range asList(v) {
					l := asList(p)
					if len(l) < 5 {
						continue
					}
					out = append(out, map[string]interface{}{"time": time.UnixMilli(int64(toFloatDefault(l[0], 0))).UTC().Format(time.RFC3339), "open": l[1], "high": l[2], "low": l[3], "close": l[4]})
				}
				return out, nil
			}},
			"event/getAll": {path: "/events", list: true, listKey: "data", query: "@options country_code type upcoming_events_only from_date to_date"},
		},
	})
}

// ---------------------------------------------------------------- Mailjet

func registerMailjet() {
	registerREST("mailjet", &restService{
		defResource: "email",
		defOps:      map[string]string{"email": "send", "sms": "send"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			r, _ := ec.str(i, "resource", "email")
			if r == "sms" {
				c, err := ec.credential("mailjetSmsApi")
				if err != nil {
					return nil, err
				}
				return bearerClient(ec.apiBase("mailjet", "https://api.mailjet.com"), c, "token"), nil
			}
			c, err := ec.credential("mailjetEmailApi")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("mailjet", "https://api.mailjet.com"), headers: map[string]string{"Authorization": basicHeader(credString(c, "apiKey"), credString(c, "secretKey"))}}, nil
		},
		ops: map[string]*restOp{
			"email/send":         {method: "POST", path: "/v3.1/send", build: mailjetMessage(false), out: "Messages"},
			"email/sendTemplate": {method: "POST", path: "/v3.1/send", build: mailjetMessage(true), out: "Messages"},
			"sms/send":           {method: "POST", path: "/v4/sms-send", body: "from:From to:To text:Text"},
		},
	})
}

func mailjetMessage(template bool) func(ec *execCtx, i int, r *restRequest) error {
	return func(ec *execCtx, i int, r *restRequest) error {
		addrs := func(s string) []interface{} {
			var out []interface{}
			for _, e := range strings.Split(s, ",") {
				if e = strings.TrimSpace(e); e != "" {
					out = append(out, map[string]interface{}{"Email": e})
				}
			}
			return out
		}
		from, _ := ec.str(i, "fromEmail", "")
		to, _ := ec.str(i, "toEmail", "")
		subject, _ := ec.str(i, "subject", "")
		msg := map[string]interface{}{"From": map[string]interface{}{"Email": from}, "To": addrs(to), "Subject": subject}
		if template {
			tid, _ := ec.num(i, "templateId", 0)
			msg["TemplateID"] = int(tid)
			msg["TemplateLanguage"] = true
			vars := map[string]interface{}{}
			for k, v := range rowsToMap(asMap(ec.mustParam(i, "variablesUi"))["variablesValues"], "name", "value") {
				vars[k] = v
			}
			if len(vars) > 0 {
				msg["Variables"] = vars
			}
		} else {
			if t, _ := ec.str(i, "text", ""); t != "" {
				msg["TextPart"] = t
			}
			if h, _ := ec.str(i, "html", ""); h != "" {
				msg["HTMLPart"] = h
			}
		}
		for k, v := range fieldsOf(ec, i, "additionalFields") {
			if restEmpty(v) {
				continue
			}
			switch k {
			case "bccEmail":
				msg["Bcc"] = addrs(toString(v))
			case "ccAddresses", "ccEmail":
				msg["Cc"] = addrs(toString(v))
			case "fromName":
				asMap(msg["From"])["Name"] = v
			case "replyTo":
				msg["ReplyTo"] = map[string]interface{}{"Email": v}
			case "priority":
				msg["Priority"] = v
			case "trackClicks":
				msg["TrackClicks"] = v
			case "trackOpens":
				msg["TrackOpens"] = v
			case "templateLanguage":
				msg["TemplateLanguage"] = v
			case "customCampaign":
				msg["CustomCampaign"] = v
			case "deduplicateCampaign":
				msg["DeduplicateCampaign"] = v
			}
		}
		r.body["Messages"] = []interface{}{msg}
		return nil
	}
}

// ---------------------------------------------------------------- Freshdesk

var (
	fdStatus   = map[string]int{"open": 2, "pending": 3, "resolved": 4, "closed": 5}
	fdPriority = map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}
	fdSource   = map[string]int{"email": 1, "portal": 2, "phone": 3, "chat": 7, "mobileHelp": 8, "feedbackWidget": 9, "outboundEmail": 10}
)

func registerFreshdesk() {
	registerREST("freshdesk", &restService{
		defResource: "ticket",
		defOps:      map[string]string{"ticket": "create", "contact": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("freshdeskApi")
			if err != nil {
				return nil, err
			}
			d := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(credString(c, "domain"), "https://"), "http://"), "/")
			if !strings.Contains(d, ".") {
				d += ".freshdesk.com"
			}
			return &restClient{base: ec.apiBase("freshdesk", "https://"+d+"/api/v2"), headers: map[string]string{"Authorization": basicHeader(credString(c, "apiKey"), "X")}}, nil
		},
		ops: map[string]*restOp{
			"ticket/create":  {method: "POST", path: "/tickets", build: fdTicket(true)},
			"ticket/update":  {method: "PUT", path: "/tickets/{ticketId}", build: fdTicket(false)},
			"ticket/get":     {path: "/tickets/{ticketId}"},
			"ticket/delete":  {method: "DELETE", path: "/tickets/{ticketId}", success: true},
			"ticket/getAll":  {path: "/tickets", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 100, query: "@options companyId:company_id include order:order_type orderBy:order_by requesterEmail:email requesterId:requester_id updatedSince:updated_since"},
			"contact/create": {method: "POST", path: "/contacts", body: "name email @additionalFields *snake"},
			"contact/update": {method: "PUT", path: "/contacts/{contactId}", body: "@contactFields *snake"},
			"contact/get":    {path: "/contacts/{contactId}"},
			"contact/delete": {method: "DELETE", path: "/contacts/{contactId}", success: true},
			"contact/getAll": {path: "/contacts", list: true, pager: pageNumber("page", 1), sizeParam: "per_page", maxSize: 100, query: "@filters *snake"},
		},
	})
}

func fdTicket(create bool) func(ec *execCtx, i int, r *restRequest) error {
	return func(ec *execCtx, i int, r *restRequest) error {
		f := fieldsOf(ec, i, "updateFields")
		if create {
			f = fieldsOf(ec, i, "options")
			req, _ := ec.str(i, "requester", "requesterId")
			val, _ := ec.param(i, "requesterIdentificationValue", "")
			key := map[string]string{"requesterId": "requester_id", "email": "email", "facebookId": "facebook_id", "phone": "phone", "twitterId": "twitter_id", "uniqueExternalId": "unique_external_id"}[req]
			if key == "requester_id" {
				n, _ := strconv.Atoi(toString(val))
				r.body[key] = n
			} else if key != "" {
				r.body[key] = val
			}
			st, _ := ec.str(i, "status", "pending")
			pr, _ := ec.str(i, "priority", "low")
			src, _ := ec.str(i, "source", "portal")
			r.body["status"] = fdStatus[st]
			r.body["priority"] = fdPriority[pr]
			r.body["source"] = fdSource[src]
		}
		for k, v := range f {
			if restEmpty(v) {
				continue
			}
			switch k {
			case "status":
				r.body[k] = fdStatus[toString(v)]
			case "priority":
				r.body[k] = fdPriority[toString(v)]
			case "source":
				r.body[k] = fdSource[toString(v)]
			case "ccEmails", "tags":
				r.body[snakeCase(k)] = splitList(toString(v))
			case "company", "group", "product", "agent", "emailConfigId":
				key := map[string]string{"company": "company_id", "group": "group_id", "product": "product_id", "agent": "responder_id", "emailConfigId": "email_config_id"}[k]
				n, _ := strconv.Atoi(toString(v))
				r.body[key] = n
			case "type":
				r.body["type"] = v
			case "requester", "requesterIdentificationValue":
				key := map[string]string{"requesterId": "requester_id", "email": "email", "phone": "phone"}[toString(f["requester"])]
				if k == "requesterIdentificationValue" && key != "" {
					r.body[key] = v
				}
			default:
				r.body[snakeCase(k)] = v
			}
		}
		return nil
	}
}

// ---------------------------------------------------------------- Microsoft To Do

func registerMicrosoftToDo() {
	todoFields := func(coll string) func(ec *execCtx, i int, r *restRequest) error {
		return func(ec *execCtx, i int, r *restRequest) error {
			for k, v := range fieldsOf(ec, i, coll) {
				if restEmpty(v) {
					continue
				}
				switch k {
				case "content":
					r.body["body"] = map[string]interface{}{"content": v, "contentType": "html"}
				case "dueDateTime", "reminderDateTime":
					r.body[k] = map[string]interface{}{"dateTime": v, "timeZone": "Etc/GMT"}
				default:
					r.body[k] = v
				}
			}
			return nil
		}
	}
	list := func(path string) *restOp {
		return &restOp{path: path, list: true, listKey: "value", pager: nextURLPager("@odata.nextLink"), sizeParam: "$top", maxSize: 100}
	}
	registerREST("microsoftToDo", &restService{
		defResource: "task",
		defOps:      map[string]string{"task": "create", "list": "create", "linkedResource": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("microsoftToDoOAuth2Api")
			if err != nil {
				return nil, err
			}
			return bearerClient(ec.apiBase("microsoftToDo", "https://graph.microsoft.com/v1.0/me/todo"), c), nil
		},
		ops: map[string]*restOp{
			"task/create":           {method: "POST", path: "/lists/{taskListId}/tasks", body: "title", build: todoFields("additionalFields")},
			"task/get":              {path: "/lists/{taskListId}/tasks/{taskId}"},
			"task/getAll":           list("/lists/{taskListId}/tasks"),
			"task/update":           {method: "PATCH", path: "/lists/{taskListId}/tasks/{taskId}", build: todoFields("updateFields")},
			"task/delete":           {method: "DELETE", path: "/lists/{taskListId}/tasks/{taskId}", success: true},
			"list/create":           {method: "POST", path: "/lists", body: "displayName"},
			"list/get":              {path: "/lists/{listId}"},
			"list/getAll":           list("/lists"),
			"list/update":           {method: "PATCH", path: "/lists/{listId}", body: "displayName"},
			"list/delete":           {method: "DELETE", path: "/lists/{listId}", success: true},
			"linkedResource/create": {method: "POST", path: "/lists/{taskListId}/tasks/{taskId}/linkedResources", body: "displayName applicationName @additionalFields *"},
			"linkedResource/get":    {path: "/lists/{taskListId}/tasks/{taskId}/linkedResources/{linkedResourceId}"},
			"linkedResource/getAll": list("/lists/{taskListId}/tasks/{taskId}/linkedResources"),
			"linkedResource/update": {method: "PATCH", path: "/lists/{taskListId}/tasks/{taskId}/linkedResources/{linkedResourceId}", body: "@updateFields *"},
			"linkedResource/delete": {method: "DELETE", path: "/lists/{taskListId}/tasks/{taskId}/linkedResources/{linkedResourceId}", success: true},
		},
	})
}

// ---------------------------------------------------------------- Raindrop

func registerRaindrop() {
	bookmark := func(coll string, create bool) func(ec *execCtx, i int, r *restRequest) error {
		return func(ec *execCtx, i int, r *restRequest) error {
			if create {
				cid, _ := ec.param(i, "collectionId", "")
				r.body["collection"] = map[string]interface{}{"$id": cid}
			}
			for k, v := range fieldsOf(ec, i, coll) {
				if restEmpty(v) {
					continue
				}
				switch k {
				case "tags":
					r.body["tags"] = splitList(toString(v))
				case "pleaseParse":
					if toBool(v) {
						r.body["pleaseParse"] = map[string]interface{}{}
					}
				case "collectionId":
					r.body["collection"] = map[string]interface{}{"$id": v}
				default:
					r.body[k] = v
				}
			}
			return nil
		}
	}
	registerREST("raindrop", &restService{
		defResource: "collection",
		defOps:      map[string]string{"collection": "create", "bookmark": "create", "tag": "delete", "user": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("raindropOAuth2Api")
			if err != nil {
				return nil, err
			}
			return bearerClient(ec.apiBase("raindrop", "https://api.raindrop.io/rest/v1"), c), nil
		},
		ops: map[string]*restOp{
			"bookmark/create":   {method: "POST", path: "/raindrop", body: "link", build: bookmark("additionalFields", true), out: "item"},
			"bookmark/update":   {method: "PUT", path: "/raindrop/{bookmarkId}", build: bookmark("updateFields", false), out: "item"},
			"bookmark/get":      {path: "/raindrop/{bookmarkId}", out: "item"},
			"bookmark/delete":   {method: "DELETE", path: "/raindrop/{bookmarkId}", success: true},
			"bookmark/getAll":   {path: "/raindrops/{collectionId}", list: true, listKey: "items", pager: pageNumber("page", 0), sizeParam: "perpage", maxSize: 50},
			"collection/create": {method: "POST", path: "/collection", body: "title @additionalFields cover:cover:csv public view parentId:parent.$id sort", out: "item"},
			"collection/update": {method: "PUT", path: "/collection/{collectionId}", body: "@updateFields title cover:cover:csv public view parentId:parent.$id sort", out: "item"},
			"collection/get":    {path: "/collection/{collectionId}", out: "item"},
			"collection/delete": {method: "DELETE", path: "/collection/{collectionId}", success: true},
			"collection/getAll": {path: "/collections", list: true, listKey: "items", build: func(ec *execCtx, i int, r *restRequest) error {
				if t, _ := ec.str(i, "type", "parent"); t == "children" {
					r.path = "/collections/childrens"
				}
				return nil
			}},
			"tag/getAll": {path: "/tags/{collectionId?}", list: true, listKey: "items", build: func(ec *execCtx, i int, r *restRequest) error {
				if id := toString(fieldsOf(ec, i, "filters")["collectionId"]); id != "" {
					r.path = "/tags/" + url.PathEscape(id)
				}
				return nil
			}},
			"tag/delete": {method: "DELETE", path: "/tags/{collectionId?}", body: "tags:tags:csv", success: true},
			"user/get": {path: "/user", out: "user", build: func(ec *execCtx, i int, r *restRequest) error {
				if self, _ := ec.boolean(i, "self", true); !self {
					id, _ := ec.str(i, "userId", "")
					r.path = "/user/" + url.PathEscape(id)
				}
				return nil
			}},
		},
	})
}

// ---------------------------------------------------------------- Medium

func registerMedium() {
	registerREST("medium", &restService{
		defResource: "post",
		defOps:      map[string]string{"post": "create", "publication": "getAll"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, _, err := ec.restCred(i, "mediumApi", map[string]string{"accessToken": "mediumApi", "oAuth2": "mediumOAuth2Api"})
			if err != nil {
				return nil, err
			}
			return bearerClient(ec.apiBase("medium", "https://api.medium.com/v1"), c, "accessToken"), nil
		},
		ops: map[string]*restOp{
			"post/create": {run: func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
				body := map[string]interface{}{}
				if err := ec.restFields(i, "title contentFormat content @additionalFields canonicalUrl license notifyFollowers publishStatus tags:tags:csv", body); err != nil {
					return nil, err
				}
				path := ""
				if pub, _ := ec.boolean(i, "publication", false); pub {
					id, _ := ec.str(i, "publicationId", "")
					path = "/publications/" + url.PathEscape(id) + "/posts"
				} else {
					me, err := c.do(ec, &restRequest{method: "GET", path: "/me", query: url.Values{}})
					if err != nil {
						return nil, err
					}
					path = "/users/" + url.PathEscape(toString(jpath(me, "data.id"))) + "/posts"
				}
				v, err := c.do(ec, &restRequest{method: "POST", path: path, query: url.Values{}, body: body})
				if err != nil {
					return nil, err
				}
				return restItems(jpath(v, "data")), nil
			}},
			"publication/getAll": {run: func(ec *execCtx, i int, _ Item, c *restClient) ([]Item, error) {
				me, err := c.do(ec, &restRequest{method: "GET", path: "/me", query: url.Values{}})
				if err != nil {
					return nil, err
				}
				v, err := c.do(ec, &restRequest{method: "GET", path: "/users/" + url.PathEscape(toString(jpath(me, "data.id"))) + "/publications", query: url.Values{}})
				if err != nil {
					return nil, err
				}
				l := asList(jpath(v, "data"))
				if all, _ := ec.boolean(i, "returnAll", false); !all {
					if n, _ := ec.num(i, "limit", 100); int(n) < len(l) {
						l = l[:int(n)]
					}
				}
				return restItems(l), nil
			}},
		},
	})
}

// ---------------------------------------------------------------- Google Books

func registerGoogleBooks() {
	shelf := func(ec *execCtx, i int, r *restRequest) error {
		if my, _ := ec.boolean(i, "myLibrary", false); my {
			r.path = strings.Replace(r.path, "/users/{userId}/", "/mylibrary/", 1)
			r.path = strings.Replace(r.path, "/users//", "/mylibrary/", 1)
		}
		return nil
	}
	userPath := func(suffix string) string { return "/users/{userId?}/bookshelves" + suffix }
	registerREST("googleBooks", &restService{
		defResource: "bookshelf",
		defOps:      map[string]string{"bookshelf": "get", "bookshelfVolume": "add", "volume": "get"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			h, err := googleAuthDef(ec, i, "https://www.googleapis.com/auth/books", "googleBooksOAuth2Api", "serviceAccount")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("googleBooks", "https://www.googleapis.com/books/v1"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"bookshelf/get":          {path: userPath("/{shelfId}"), build: shelf},
			"bookshelf/getAll":       {path: userPath(""), list: true, listKey: "items", build: shelf},
			"bookshelfVolume/add":    {method: "POST", path: "/mylibrary/bookshelves/{shelfId}/addVolume", query: "volumeId", success: true},
			"bookshelfVolume/clear":  {method: "POST", path: "/mylibrary/bookshelves/{shelfId}/clearVolumes", success: true},
			"bookshelfVolume/remove": {method: "POST", path: "/mylibrary/bookshelves/{shelfId}/removeVolume", query: "volumeId", success: true},
			"bookshelfVolume/move":   {method: "POST", path: "/mylibrary/bookshelves/{shelfId}/moveVolume", query: "volumeId volumePosition", success: true},
			"bookshelfVolume/getAll": {path: userPath("/{shelfId}/volumes"), list: true, listKey: "items", pager: offsetPager("startIndex"), sizeParam: "maxResults", maxSize: 40, build: shelf},
			"volume/get":             {path: "/volumes/{volumeId}"},
			"volume/getAll":          {path: "/volumes", query: "searchQuery:q", list: true, listKey: "items", pager: offsetPager("startIndex"), sizeParam: "maxResults", maxSize: 40},
		},
	})
}

// ---------------------------------------------------------------- Google Cloud Natural Language

func registerGoogleNaturalLanguage() {
	registerREST("googleCloudNaturalLanguage", &restService{
		defResource: "document",
		defOps:      map[string]string{"document": "analyzeSentiment"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			h, err := googleAuthDef(ec, i, "https://www.googleapis.com/auth/cloud-language https://www.googleapis.com/auth/cloud-platform", "googleCloudNaturalLanguageOAuth2Api", "oAuth2")
			if err != nil {
				return nil, err
			}
			return &restClient{base: ec.apiBase("googleCloudNaturalLanguage", "https://language.googleapis.com/v1"), headers: h}, nil
		},
		ops: map[string]*restOp{
			"document/analyzeSentiment": {method: "POST", path: "/documents:analyzeSentiment", build: func(ec *execCtx, i int, r *restRequest) error {
				src, _ := ec.str(i, "source", "content")
				o := fieldsOf(ec, i, "options")
				doc := map[string]interface{}{"type": firstNonEmpty(toString(o["documentType"]), "PLAIN_TEXT")}
				if src == "gcsContentUri" {
					u, _ := ec.str(i, "gcsContentUri", "")
					doc["gcsContentUri"] = u
				} else {
					c, _ := ec.str(i, "content", "")
					doc["content"] = c
				}
				if l := toString(o["language"]); l != "" {
					doc["language"] = l
				}
				r.body["document"] = doc
				r.body["encodingType"] = firstNonEmpty(toString(o["encodingType"]), "UTF16")
				return nil
			}},
		},
	})
}

// ---------------------------------------------------------------- Intercom

func registerIntercom() {
	ident := func(ec *execCtx, i int, r *restRequest) error {
		t, _ := ec.str(i, "identifierType", "email")
		v, _ := ec.param(i, "idValue", "")
		if !restEmpty(v) {
			r.body[t] = v
		}
		f := fieldsOf(ec, i, "additionalFields")
		if cf, ok := f["customAttributesUi"]; ok {
			delete(r.body, "customAttributesUi")
			if m := rowsToMap(asMap(cf)["customAttributesValues"], "name", "value"); len(m) > 0 {
				r.body["custom_attributes"] = m
			}
		}
		if comps, ok := r.body["companies"]; ok {
			var out []interface{}
			for _, c := range splitList(toString(comps)) {
				out = append(out, map[string]interface{}{"company_id": c})
			}
			r.body["companies"] = out
		}
		return nil
	}
	lookup := func(kind string) func(ec *execCtx, i int, r *restRequest) error {
		return func(ec *execCtx, i int, r *restRequest) error {
			by, _ := ec.str(i, "selectBy", "id")
			v, _ := ec.str(i, "value", "")
			switch by {
			case "id":
				r.path = "/" + kind + "/" + url.PathEscape(v)
			default:
				r.path = "/" + kind
				r.query.Set(by, v)
			}
			return nil
		}
	}
	registerREST("intercom", &restService{
		defResource: "user",
		defOps:      map[string]string{"user": "create", "lead": "create", "company": "create"},
		client: func(ec *execCtx, i int) (*restClient, error) {
			c, err := ec.credential("intercomApi")
			if err != nil {
				return nil, err
			}
			cl := bearerClient(ec.apiBase("intercom", "https://api.intercom.io"), c, "apiKey")
			cl.headers["Accept"] = "application/json"
			return cl, nil
		},
		ops: map[string]*restOp{
			"user/create":    {method: "POST", path: "/users", body: "@additionalFields *snake", build: ident},
			"user/update":    {method: "POST", path: "/users", body: "@additionalFields *snake", build: ident},
			"user/get":       {path: "/users", build: lookup("users")},
			"user/delete":    {method: "DELETE", path: "/users/{id}"},
			"user/getAll":    {path: "/users", list: true, listKey: "users", pager: nextURLPager("pages.next"), sizeParam: "per_page", maxSize: 60, query: "@filters *snake"},
			"lead/create":    {method: "POST", path: "/contacts", body: "email @additionalFields *snake", build: ident},
			"lead/update":    {method: "POST", path: "/contacts", body: "@additionalFields *snake", build: ident},
			"lead/get":       {path: "/contacts", build: lookup("contacts")},
			"lead/delete":    {method: "DELETE", path: "/contacts", build: lookup("contacts")},
			"lead/getAll":    {path: "/contacts", list: true, listKey: "contacts", pager: nextURLPager("pages.next"), sizeParam: "per_page", maxSize: 60, query: "@filters *snake"},
			"company/create": {method: "POST", path: "/companies", body: "companyId:company_id @additionalFields *snake", build: ident},
			"company/update": {method: "POST", path: "/companies", body: "companyId:company_id @additionalFields *snake", build: ident},
			"company/get":    {path: "/companies", build: lookup("companies")},
			"company/getAll": {path: "/companies", list: true, listKey: "companies", pager: nextURLPager("pages.next"), sizeParam: "per_page", maxSize: 60, query: "@filters *snake"},
			"company/users":  {path: "/companies/{value}/users", list: true, listKey: "users"},
		},
	})
}
