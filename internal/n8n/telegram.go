package n8n

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/dgmos/flow/internal/node"
)

var regexpBotToken = regexp.MustCompile(`/bot[^/]+/`)

func telegramNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("telegramApi")
	if err != nil {
		return nil, err
	}
	token := credString(c, "accessToken")
	base := strings.TrimRight(credString(c, "baseUrl"), "/")
	if base == "" {
		base = ec.apiBase("telegram", "https://api.telegram.org")
	}
	var out []Item
	for i, it := range ec.items() {
		resource, _ := ec.str(i, "resource", "message")
		op, _ := ec.str(i, "operation", map[string]string{"message": "sendMessage", "file": "get", "chat": "get", "callback": "answerQuery"}[resource])
		res, bin, err := telegramOp(ec, i, it, base, token, resource, op)
		if err != nil {
			return nil, fmt.Errorf("telegram %s/%s: %w", resource, op, err)
		}
		out = append(out, Item{JSON: res, Binary: bin, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

func telegramOp(ec *execCtx, i int, it Item, base, token, resource, op string) (map[string]interface{}, map[string]interface{}, error) {
	call := func(method string, body map[string]interface{}) (map[string]interface{}, error) {
		res, _, err := ec.apiCall("POST", base+"/bot"+token+"/"+method, nil, body, "")
		return res, err
	}
	add := func(body map[string]interface{}) error {
		af, err := ec.param(i, "additionalFields", map[string]interface{}{})
		if err != nil {
			return err
		}
		m, _ := af.(map[string]interface{})
		for k, v := range m {
			switch k {
			case "appendAttribution", "fileName":
			case "parse_mode", "disable_notification", "disable_web_page_preview", "reply_to_message_id", "message_thread_id", "caption", "duration", "width", "height", "protect_content":
				if v != "" && v != nil {
					body[k] = v
				}
			}
		}
		if rm, err := telegramReplyMarkup(ec, i); err != nil {
			return err
		} else if rm != nil {
			body["reply_markup"] = rm
		}
		return nil
	}
	chatID, _ := ec.str(i, "chatId", "")
	switch resource + "." + op {
	case "message.sendMessage":
		text, err := ec.str(i, "text", "")
		if err != nil {
			return nil, nil, err
		}
		body := map[string]interface{}{"chat_id": chatID, "text": text}
		if err := add(body); err != nil {
			return nil, nil, err
		}
		res, err := call("sendMessage", body)
		return res, nil, err
	case "message.sendAndWait":
		a, err := ec.sendAndWaitApproval(i)
		if err != nil {
			return nil, nil, err
		}
		text, _ := ec.str(i, "message", "")
		row := []map[string]interface{}{{"text": a.approveLabel, "url": a.approveURL}}
		if a.double {
			row = append(row, map[string]interface{}{"text": a.declineLabel, "url": a.declineURL})
		}
		if _, err := call("sendMessage", map[string]interface{}{"chat_id": chatID, "text": text, "reply_markup": map[string]interface{}{"inline_keyboard": [][]map[string]interface{}{row}}}); err != nil {
			return nil, nil, err
		}
		return nil, nil, errWaitSignal
	case "message.sendChatAction":
		action, _ := ec.str(i, "action", "typing")
		res, err := call("sendChatAction", map[string]interface{}{"chat_id": chatID, "action": action})
		return res, nil, err
	case "message.editMessageText":
		text, _ := ec.str(i, "text", "")
		body := map[string]interface{}{"chat_id": chatID, "text": text}
		if mid, _ := ec.str(i, "messageId", ""); mid != "" {
			body["message_id"] = mid
		}
		if err := add(body); err != nil {
			return nil, nil, err
		}
		res, err := call("editMessageText", body)
		return res, nil, err
	case "message.deleteMessage":
		mid, _ := ec.str(i, "messageId", "")
		res, err := call("deleteMessage", map[string]interface{}{"chat_id": chatID, "message_id": mid})
		return res, nil, err
	case "message.pinChatMessage", "message.unpinChatMessage":
		mid, _ := ec.str(i, "messageId", "")
		res, err := call(op, map[string]interface{}{"chat_id": chatID, "message_id": mid})
		return res, nil, err
	case "message.sendLocation":
		lat, _ := ec.num(i, "latitude", 0)
		lon, _ := ec.num(i, "longitude", 0)
		res, err := call("sendLocation", map[string]interface{}{"chat_id": chatID, "latitude": lat, "longitude": lon})
		return res, nil, err
	case "message.sendPhoto", "message.sendDocument", "message.sendVideo", "message.sendAudio", "message.sendAnimation", "message.sendSticker":
		field := strings.ToLower(strings.TrimPrefix(op, "send"))
		body := map[string]interface{}{"chat_id": chatID}
		if err := add(body); err != nil {
			return nil, nil, err
		}
		if isBin, _ := ec.boolean(i, "binaryData", false); isBin {
			prop, _ := ec.str(i, "binaryPropertyName", "data")
			data, mt, name, err := binaryOf(it, prop)
			if err != nil {
				return nil, nil, err
			}
			if fn, _ := ec.str(i, "additionalFields.fileName", ""); fn != "" {
				name = fn
			}
			res, err := telegramUpload(ec, base+"/bot"+token+"/"+op, body, field, data, mt, name)
			return res, nil, err
		}
		file, _ := ec.str(i, "file", "")
		body[field] = file
		res, err := call(op, body)
		return res, nil, err
	case "file.get":
		fileID, _ := ec.str(i, "fileId", "")
		res, err := call("getFile", map[string]interface{}{"file_id": fileID})
		if err != nil {
			return nil, nil, err
		}
		if dl, _ := ec.boolean(i, "download", true); !dl {
			return res, nil, nil
		}
		result, _ := res["result"].(map[string]interface{})
		fp := toString(result["file_path"])
		if fp == "" {
			return nil, nil, fmt.Errorf("getFile returned no file_path")
		}
		data, mt, err := ec.download(base + "/file/bot" + token + "/" + fp)
		if err != nil {
			return nil, nil, err
		}
		if mt == "" || mt == "application/octet-stream" {
			mt = mimeByExt(fp)
		}
		return res, map[string]interface{}{"data": binaryData(data, mt, path.Base(fp))}, nil
	case "chat.get", "chat.leave", "chat.member":
		method := map[string]string{"get": "getChat", "leave": "leaveChat", "member": "getChatMember"}[op]
		body := map[string]interface{}{"chat_id": chatID}
		if op == "member" {
			uid, _ := ec.str(i, "userId", "")
			body["user_id"] = uid
		}
		res, err := call(method, body)
		return res, nil, err
	case "callback.answerQuery":
		qid, _ := ec.str(i, "queryId", "")
		body := map[string]interface{}{"callback_query_id": qid}
		if af, _ := ec.param(i, "additionalFields", nil); af != nil {
			for k, v := range asMap(af) {
				body[k] = v
			}
		}
		res, err := call("answerCallbackQuery", body)
		return res, nil, err
	}
	return nil, nil, permanentError{fmt.Sprintf("operation %s/%s is not supported", resource, op)}
}

func telegramReplyMarkup(ec *execCtx, i int) (interface{}, error) {
	rm, _ := ec.str(i, "replyMarkup", "none")
	switch rm {
	case "", "none":
		return nil, nil
	case "forceReply":
		return map[string]interface{}{"force_reply": true}, nil
	case "replyKeyboardRemove":
		return map[string]interface{}{"remove_keyboard": true}, nil
	case "inlineKeyboard", "replyKeyboard":
		key := map[string]string{"inlineKeyboard": "inlineKeyboard", "replyKeyboard": "replyKeyboard"}[rm]
		v, err := ec.param(i, key+".rows", nil)
		if err != nil {
			return nil, err
		}
		var rows [][]map[string]interface{}
		for _, r := range asList(v) {
			row, _ := r.(map[string]interface{})
			btns, _ := row["row"].(map[string]interface{})
			var out []map[string]interface{}
			for _, b := range asList(btns["buttons"]) {
				bm, _ := b.(map[string]interface{})
				btn := map[string]interface{}{"text": bm["text"]}
				if af, ok := bm["additionalFields"].(map[string]interface{}); ok {
					for k, x := range af {
						btn[k] = x
					}
				}
				out = append(out, btn)
			}
			rows = append(rows, out)
		}
		if rm == "inlineKeyboard" {
			return map[string]interface{}{"inline_keyboard": rows}, nil
		}
		return map[string]interface{}{"keyboard": rows}, nil
	}
	return nil, nil
}

func telegramUpload(ec *execCtx, u string, fields map[string]interface{}, fileField string, data []byte, mt, name string) (map[string]interface{}, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		val := toString(v)
		_ = w.WriteField(k, val)
	}
	part, err := w.CreateFormFile(fileField, name)
	if err != nil {
		return nil, err
	}
	_, _ = part.Write(data)
	_ = w.Close()
	res, _, err := ec.apiCall("POST", u, nil, buf.Bytes(), w.FormDataContentType())
	return res, err
}

// download fetches a URL through the outbound policy.
func (ec *execCtx) download(u string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ec.ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := outboundClient(ec.opts.Outbound).Do(req)
	if err != nil {
		return nil, "", sanitizeErr(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := node.ReadLimited(resp.Body, ec.opts.Outbound.MaxResponseBytesOrDefault())
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode >= 400 {
		return nil, "", fmt.Errorf("download failed with status %d", resp.StatusCode)
	}
	return data, resp.Header.Get("Content-Type"), nil
}

func mimeByExt(p string) string {
	switch strings.ToLower(path.Ext(p)) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".oga", ".ogg":
		return "audio/ogg"
	case ".mp3":
		return "audio/mpeg"
	case ".pdf":
		return "application/pdf"
	}
	return "application/octet-stream"
}
