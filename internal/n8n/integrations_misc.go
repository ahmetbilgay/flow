package n8n

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------- RSS Feed Trigger

// rssFeedReadTriggerNode emits feed items newer than the newest item seen
// by the previous poll; the first poll only records it.
func rssFeedReadTriggerNode(ec *execCtx) ([][]Item, error) {
	u, err := ec.str(0, "feedUrl", "")
	if err != nil {
		return nil, err
	}
	items, err := ec.readFeed(u)
	if err != nil {
		return nil, err
	}
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	date := func(it Item) time.Time {
		t, _ := timeOf(firstNonEmpty(toString(it.JSON["isoDate"]), toString(it.JSON["pubDate"])))
		return t
	}
	newest := time.Time{}
	if pc.seen {
		newest, _ = time.Parse(time.RFC3339Nano, pc.prev)
	}
	last := newest
	var out []Item
	for _, it := range items {
		d := date(it)
		if pc.seen && d.After(last) {
			out = append(out, it)
		}
		if d.After(newest) {
			newest = d
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return date(out[a]).Before(date(out[b])) })
	return pc.commit(newest.UTC().Format(time.RFC3339Nano), out)
}

// ---------------------------------------------------------------- DHL

func dhlNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("dhlApi")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("dhl", "https://api-eu.dhl.com")
	h := map[string]string{"DHL-API-Key": credString(c, "apiKey")}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "shipment", "get")
		if r != "shipment" || o != "get" {
			return nil, unsupportedOp("DHL", r, o)
		}
		num, _ := ec.str(i, "trackingNumber", "")
		q := url.Values{"trackingNumber": {num}}
		if pc, _ := ec.str(i, "options.recipientsPostalCode", ""); pc != "" {
			q.Set("recipientPostalCode", pc)
		}
		res, _, err := ec.apiCall("GET", base+"/track/shipments?"+q.Encode(), h, nil, "")
		if err != nil {
			return nil, err
		}
		return listItems(res["shipments"]), nil
	})
}

// ---------------------------------------------------------------- n8n (public API)

func n8nAPINode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("n8nApi")
	if err != nil {
		return nil, err
	}
	base := strings.TrimRight(credBaseURL(c, ""), "/")
	if !strings.HasSuffix(base, "/api/v1") {
		base += "/api/v1"
	}
	h := map[string]string{"X-N8N-API-KEY": credString(c, "apiKey")}
	call := func(method, path string, body interface{}) (map[string]interface{}, error) {
		res, _, err := ec.apiCall(method, base+path, h, body, "")
		return res, err
	}
	list := func(i int, path string, q url.Values) ([]Item, error) {
		all, _ := ec.boolean(i, "returnAll", true)
		l, _ := ec.num(i, "limit", 100)
		limit := int(l)
		var out []Item
		for page := 0; page < 1000; page++ {
			q.Set("limit", "100")
			res, err := call("GET", path+"?"+q.Encode(), nil)
			if err != nil {
				return nil, err
			}
			out = append(out, listItems(res["data"])...)
			next := toString(res["nextCursor"])
			if next == "" || (!all && len(out) >= limit) {
				break
			}
			q.Set("cursor", next)
		}
		return capItems(out, all, limit), nil
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "workflow", map[string]string{"workflow": "getAll", "execution": "getAll", "credential": "create", "audit": "generate"})
		id := func(key string) string { v, _ := ec.rlString(i, key); return url.PathEscape(v) }
		switch r + "." + o {
		case "workflow.getAll":
			q := url.Values{}
			for k, v := range fieldsOf(ec, i, "filters") {
				switch k {
				case "activeWorkflows":
					q.Set("active", strconv.FormatBool(toBool(v)))
				case "tags", "name", "projectId":
					q.Set(k, toString(v))
				case "excludePinnedData":
					q.Set(k, strconv.FormatBool(toBool(v)))
				}
			}
			return list(i, "/workflows", q)
		case "workflow.get":
			res, err := call("GET", "/workflows/"+id("workflowId"), nil)
			return one(res), err
		case "workflow.activate", "workflow.deactivate":
			res, err := call("POST", "/workflows/"+id("workflowId")+"/"+o, nil)
			return one(res), err
		case "workflow.delete":
			res, err := call("DELETE", "/workflows/"+id("workflowId"), nil)
			return one(res), err
		case "workflow.create", "workflow.update":
			v, _ := ec.param(i, "workflowObject", "{}")
			body := parseJSONParam(v)
			if o == "create" {
				res, err := call("POST", "/workflows", body)
				return one(res), err
			}
			res, err := call("PUT", "/workflows/"+id("workflowId"), body)
			return one(res), err
		case "execution.getAll":
			q := url.Values{}
			for k, v := range fieldsOf(ec, i, "filters") {
				if k == "workflowId" {
					v = rlValue(v)
				}
				q.Set(k, toString(v))
			}
			if inc, _ := ec.boolean(i, "options.activeWorkflows", false); inc {
				q.Set("includeData", "true")
			}
			return list(i, "/executions", q)
		case "execution.get", "execution.delete":
			method := map[string]string{"get": "GET", "delete": "DELETE"}[o]
			q := ""
			if inc, _ := ec.boolean(i, "options.includeData", false); inc && o == "get" {
				q = "?includeData=true"
			}
			res, err := call(method, "/executions/"+id("executionId")+q, nil)
			return one(res), err
		case "credential.create":
			name, _ := ec.str(i, "name", "")
			typ, _ := ec.str(i, "credentialTypeName", "")
			v, _ := ec.param(i, "data", "{}")
			res, err := call("POST", "/credentials", map[string]interface{}{"name": name, "type": typ, "data": parseJSONParam(v)})
			return one(res), err
		case "credential.delete":
			res, err := call("DELETE", "/credentials/"+id("credentialId"), nil)
			return one(res), err
		case "credential.getSchema":
			typ, _ := ec.str(i, "credentialTypeName", "")
			res, err := call("GET", "/credentials/schema/"+url.PathEscape(typ), nil)
			return one(res), err
		case "audit.generate":
			res, err := call("POST", "/audit", map[string]interface{}{"additionalOptions": fieldsOf(ec, i, "additionalOptions")})
			return one(res), err
		}
		return nil, unsupportedOp("n8n", r, o)
	})
}
