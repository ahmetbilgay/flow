package n8n

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"
)

// Writers for Convert to File / Spreadsheet File formats beyond CSV and
// XLSX: OpenDocument spreadsheets, RTF tables and iCalendar events.

func xmlText(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// odsParagraphs renders cell text as <text:p> elements, preserving runs of
// spaces, tabs and line breaks the way OpenDocument requires.
func odsParagraphs(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n") {
		b.WriteString("<text:p>")
		spaces := 0
		flush := func() {
			switch {
			case spaces == 1:
				b.WriteByte(' ')
			case spaces > 1:
				b.WriteByte(' ')
				fmt.Fprintf(&b, `<text:s text:c="%d"/>`, spaces-1)
			}
			spaces = 0
		}
		start := true
		for _, r := range line {
			switch {
			case r == ' ' && start:
				b.WriteString("<text:s/>") // leading spaces are not collapsed
			case r == ' ':
				spaces++
			case r == '\t':
				flush()
				b.WriteString("<text:tab/>")
			default:
				flush()
				b.WriteString(xmlText(string(r)))
			}
			if r != ' ' {
				start = false
			}
		}
		flush()
		b.WriteString("</text:p>")
	}
	return b.String()
}

// writeODS renders items as a one-sheet OpenDocument spreadsheet.
func writeODS(items []Item, sheet string, header bool) ([]byte, error) {
	if sheet == "" {
		sheet = "Sheet"
	}
	cols := columnsOf(items)
	var c strings.Builder
	c.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" office:version="1.2"><office:body><office:spreadsheet>`)
	fmt.Fprintf(&c, `<table:table table:name="%s">`, xmlText(sheet))
	if len(cols) > 0 {
		fmt.Fprintf(&c, `<table:table-column table:number-columns-repeated="%d"/>`, len(cols))
	}
	str := func(s string) {
		c.WriteString(`<table:table-cell office:value-type="string">` + odsParagraphs(s) + `</table:table-cell>`)
	}
	if header {
		c.WriteString("<table:table-row>")
		for _, k := range cols {
			str(k)
		}
		c.WriteString("</table:table-row>")
	}
	for _, it := range items {
		c.WriteString("<table:table-row>")
		for _, k := range cols {
			switch v := it.JSON[k].(type) {
			case nil:
				c.WriteString("<table:table-cell/>")
			case float64:
				if math.IsInf(v, 0) || math.IsNaN(v) {
					str(cellString(v))
					continue
				}
				n := strconv.FormatFloat(v, 'g', -1, 64)
				fmt.Fprintf(&c, `<table:table-cell office:value-type="float" office:value="%s"><text:p>%s</text:p></table:table-cell>`, n, strconv.FormatFloat(v, 'f', -1, 64))
			case bool:
				fmt.Fprintf(&c, `<table:table-cell office:value-type="boolean" office:boolean-value="%t"><text:p>%s</text:p></table:table-cell>`, v, strings.ToUpper(strconv.FormatBool(v)))
			case string:
				if v == "" {
					c.WriteString("<table:table-cell/>")
				} else {
					str(v)
				}
			default:
				str(cellString(v))
			}
		}
		c.WriteString("</table:table-row>")
	}
	c.WriteString("</table:table></office:spreadsheet></office:body></office:document-content>")

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// The mimetype entry comes first, stored, without a data descriptor.
	mime := []byte("application/vnd.oasis.opendocument.spreadsheet")
	w, err := zw.CreateRaw(&zip.FileHeader{Name: "mimetype", Method: zip.Store, CRC32: crc32.ChecksumIEEE(mime),
		CompressedSize64: uint64(len(mime)), UncompressedSize64: uint64(len(mime))})
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(mime); err != nil {
		return nil, err
	}
	files := []struct{ name, body string }{
		{"META-INF/manifest.xml", `<?xml version="1.0" encoding="UTF-8"?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.2"><manifest:file-entry manifest:full-path="/" manifest:version="1.2" manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/><manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/><manifest:file-entry manifest:full-path="styles.xml" manifest:media-type="text/xml"/></manifest:manifest>`},
		{"styles.xml", `<?xml version="1.0" encoding="UTF-8"?>
<office:document-styles xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" office:version="1.2"/>`},
		{"content.xml", c.String()},
	}
	for _, f := range files {
		w, err := zw.Create(f.name)
		if err != nil {
			return nil, err
		}
		if _, err := w.Write([]byte(f.body)); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// rtfText escapes text for RTF; non-ASCII characters become \uN? escapes.
func rtfText(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\\' || r == '{' || r == '}':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\line `)
		case r == '\t':
			b.WriteString(`\tab `)
		case r == '\r':
		case r < 0x80:
			b.WriteRune(r)
		default:
			for _, u := range utf16.Encode([]rune{r}) {
				fmt.Fprintf(&b, `\u%d?`, int16(u))
			}
		}
	}
	return b.String()
}

// writeRTF renders items as an RTF table.
func writeRTF(items []Item, header bool) []byte {
	cols := columnsOf(items)
	var b strings.Builder
	b.WriteString(`{\rtf1\ansi\ansicpg1252\deff0{\fonttbl{\f0\fswiss Arial;}}\f0\fs20` + "\n")
	row := func(vals []string, bold bool) {
		b.WriteString(`\trowd\trgaph108`)
		for i := range vals {
			fmt.Fprintf(&b, `\cellx%d`, 2000*(i+1))
		}
		b.WriteString("\n")
		for _, v := range vals {
			b.WriteString(`\pard\intbl `)
			if bold {
				b.WriteString(`{\b ` + rtfText(v) + `}`)
			} else {
				b.WriteString(rtfText(v))
			}
			b.WriteString(`\cell` + "\n")
		}
		b.WriteString(`\row` + "\n")
	}
	if len(cols) > 0 {
		if header {
			row(cols, true)
		}
		for _, it := range items {
			vals := make([]string, len(cols))
			for c, k := range cols {
				vals[c] = cellString(it.JSON[k])
			}
			row(vals, false)
		}
	}
	b.WriteString(`\pard}`)
	return []byte(b.String())
}

// readRTFTable reads the rows of the tables in an RTF document (and any
// text outside tables as single-cell rows), like SheetJS does.
func readRTFTable(data []byte) ([][]string, error) {
	s := string(data)
	if !strings.HasPrefix(strings.TrimSpace(s), `{\rtf`) {
		return nil, permanentError{"file is not an RTF document"}
	}
	var (
		rows   [][]string
		row    []string
		cell   strings.Builder
		inRow  bool
		depth  int
		skipAt = -1 // group depth of an ignored destination
		uc     = 1
		skipN  int
	)
	// Destinations whose text is not document content.
	ignored := map[string]bool{"fonttbl": true, "colortbl": true, "stylesheet": true, "info": true, "pict": true, "object": true,
		"header": true, "footer": true, "listtable": true, "listoverridetable": true, "rsidtbl": true, "generator": true, "xmlnstbl": true, "themedata": true, "colorschememapping": true, "latentstyles": true, "datastore": true}
	endPara := func() {
		if !inRow && strings.TrimSpace(cell.String()) != "" {
			if len(rows) >= maxSheetRows {
				return
			}
			rows = append(rows, []string{strings.TrimSpace(cell.String())})
		}
		if !inRow {
			cell.Reset()
		}
	}
	// write adds one character of document text, honoring the characters
	// a \uN escape says to skip (its ANSI fallback).
	write := func(t string) {
		if skipAt >= 0 {
			return
		}
		if skipN > 0 {
			skipN--
			return
		}
		cell.WriteString(t)
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch ch {
		case '{':
			depth++
		case '}':
			if depth == skipAt {
				skipAt = -1
			}
			depth--
		case '\r', '\n':
		case '\\':
			if i+1 >= len(s) {
				break
			}
			n := s[i+1]
			if n == '\\' || n == '{' || n == '}' {
				write(string(n))
				i++
				continue
			}
			if n == '*' {
				if skipAt < 0 {
					skipAt = depth
				}
				i++
				continue
			}
			if n == '\'' && i+3 < len(s) {
				if v, err := strconv.ParseUint(s[i+2:i+4], 16, 8); err == nil {
					if skipN > 0 {
						skipN--
					} else if skipAt < 0 {
						cell.WriteRune(rune(v)) // Windows-1252 approximated as Latin-1
					}
				}
				i += 3
				continue
			}
			j := i + 1
			for j < len(s) && (s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z') {
				j++
			}
			word := s[i+1 : j]
			k := j
			if k < len(s) && (s[k] == '-' || s[k] >= '0' && s[k] <= '9') {
				k++
				for k < len(s) && s[k] >= '0' && s[k] <= '9' {
					k++
				}
			}
			param := s[j:k]
			if k < len(s) && s[k] == ' ' {
				k++
			}
			if word == "" { // control symbol such as \~ or \-
				if n == '~' {
					write(" ")
				}
				i++
				continue
			}
			i = k - 1
			if ignored[word] && skipAt < 0 {
				skipAt = depth
				continue
			}
			if skipAt >= 0 {
				continue
			}
			switch word {
			case "uc":
				uc, _ = strconv.Atoi(param)
			case "u":
				v, _ := strconv.Atoi(param)
				if v < 0 {
					v += 65536
				}
				cell.WriteRune(rune(v))
				skipN = uc
			case "par", "line":
				if inRow {
					cell.WriteByte('\n')
				} else {
					endPara()
				}
			case "tab":
				cell.WriteByte('\t')
			case "trowd":
				if !inRow {
					endPara()
				}
				inRow, row = true, nil
				cell.Reset()
			case "cell":
				row = append(row, strings.TrimSpace(cell.String()))
				cell.Reset()
			case "row":
				if len(rows) >= maxSheetRows {
					return nil, permanentError{fmt.Sprintf("table has more than %d rows", maxSheetRows)}
				}
				rows = append(rows, row)
				inRow, row = false, nil
				cell.Reset()
			}
		default:
			write(s[i : i+1])
		}
	}
	endPara()
	return rows, nil
}

// icsEscape escapes an iCalendar TEXT value.
func icsEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, ";", `\;`, ",", `\,`, "\r\n", `\n`, "\n", `\n`)
	return r.Replace(s)
}

// icsFold folds content lines at 75 octets.
func icsFold(line string) string {
	if len(line) <= 75 {
		return line + "\r\n"
	}
	var b strings.Builder
	n := 0
	for _, r := range line {
		l := len(string(r))
		if n+l > 75 {
			b.WriteString("\r\n ")
			n = 1
		}
		b.WriteRune(r)
		n += l
	}
	return b.String() + "\r\n"
}

func icsTime(s string, allDay bool, loc *time.Location) (string, error) {
	var t time.Time
	var err error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04:05", "2006-01-02"} {
		if layout == time.RFC3339Nano {
			t, err = time.Parse(layout, s)
		} else {
			t, err = time.ParseInLocation(layout, s, loc)
		}
		if err == nil {
			break
		}
	}
	if err != nil {
		return "", permanentError{fmt.Sprintf("invalid date %q", s)}
	}
	if allDay {
		return t.In(loc).Format("20060102"), nil
	}
	return t.UTC().Format("20060102T150405Z"), nil
}

// icalEvent builds the .ics file of the Convert to File "iCal" operation
// for item i.
func icalEvent(ec *execCtx, i int) ([]byte, error) {
	loc := time.UTC
	if z := ec.data.opts.timezone(ec.data.wf); z != "" {
		if l, err := time.LoadLocation(z); err == nil {
			loc = l
		}
	}
	title, _ := ec.str(i, "title", "")
	start, _ := ec.str(i, "start", "")
	end, _ := ec.str(i, "end", "")
	allDay, _ := ec.boolean(i, "allDay", false)
	if start == "" {
		start = time.Now().In(loc).Format(time.RFC3339)
	}
	if end == "" {
		if t, err := time.Parse(time.RFC3339, start); err == nil {
			end = t.Add(time.Hour).Format(time.RFC3339)
		} else {
			end = start
		}
	}
	ds, err := icsTime(start, allDay, loc)
	if err != nil {
		return nil, err
	}
	de, err := icsTime(end, allDay, loc)
	if err != nil {
		return nil, err
	}
	if allDay && de == ds {
		if t, err := time.ParseInLocation("20060102", ds, loc); err == nil {
			de = t.AddDate(0, 0, 1).Format("20060102")
		}
	}
	get := func(k string) string { v, _ := ec.str(i, "additionalFields."+k, ""); return v }
	calName := get("calName")
	var b strings.Builder
	line := func(s string) { b.WriteString(icsFold(s)) }
	line("BEGIN:VCALENDAR")
	line("VERSION:2.0")
	line("CALSCALE:GREGORIAN")
	line("PRODID:-//flow//n8n//EN")
	line("METHOD:PUBLISH")
	if calName != "" {
		line("X-WR-CALNAME:" + icsEscape(calName))
	}
	line("BEGIN:VEVENT")
	uid := get("uid")
	if uid == "" {
		uid = uuid.NewString()
	}
	line("UID:" + icsEscape(uid))
	line("SUMMARY:" + icsEscape(title))
	line("DTSTAMP:" + time.Now().UTC().Format("20060102T150405Z"))
	if allDay {
		line("DTSTART;VALUE=DATE:" + ds)
		line("DTEND;VALUE=DATE:" + de)
	} else {
		line("DTSTART:" + ds)
		line("DTEND:" + de)
	}
	if v := get("description"); v != "" {
		line("DESCRIPTION:" + icsEscape(v))
	}
	if v := get("location"); v != "" {
		line("LOCATION:" + icsEscape(v))
	}
	if v := get("url"); v != "" {
		line("URL:" + v)
	}
	if v := get("status"); v != "" {
		line("STATUS:" + strings.ToUpper(v))
	}
	if v := get("busyStatus"); v != "" {
		line("X-MICROSOFT-CDO-BUSYSTATUS:" + strings.ToUpper(v))
	}
	if v := get("recurrenceRule"); v != "" {
		line("RRULE:" + strings.TrimPrefix(v, "RRULE:"))
	}
	if v, _ := ec.num(i, "additionalFields.sequence", -1); v >= 0 {
		line("SEQUENCE:" + strconv.Itoa(int(v)))
	}
	geo, _ := ec.param(i, "additionalFields.geolocationUi.values", nil)
	if g, ok := geo.(map[string]interface{}); ok && toString(g["lat"]) != "" {
		line(fmt.Sprintf("GEO:%s;%s", toString(g["lat"]), toString(g["lon"])))
	}
	org, _ := ec.param(i, "additionalFields.organizerUi.values", nil)
	if o, ok := org.(map[string]interface{}); ok && toString(o["email"]) != "" {
		line(fmt.Sprintf("ORGANIZER;CN=%s:mailto:%s", icsParam(toString(o["name"])), toString(o["email"])))
	}
	att, _ := ec.param(i, "additionalFields.attendeesUi.values", nil)
	for _, a := range asList(att) {
		m, ok := a.(map[string]interface{})
		if !ok || toString(m["email"]) == "" {
			continue
		}
		rsvp := "FALSE"
		if toBool(m["rsvp"]) {
			rsvp = "TRUE"
		}
		line(fmt.Sprintf("ATTENDEE;RSVP=%s;CN=%s:mailto:%s", rsvp, icsParam(toString(m["name"])), toString(m["email"])))
	}
	line("END:VEVENT")
	line("END:VCALENDAR")
	return []byte(b.String()), nil
}

// icsParam quotes a parameter value when needed.
func icsParam(s string) string {
	s = strings.ReplaceAll(s, `"`, "'")
	if strings.ContainsAny(s, ";:,") {
		return `"` + s + `"`
	}
	return s
}
