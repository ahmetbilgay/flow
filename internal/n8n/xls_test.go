package n8n

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

// testdata/libreoffice.xls was saved by LibreOffice Calc: a shared string
// table, number formats (date, percentage), formulas, booleans, an error
// value and two sheets.
func TestReadXLSLibreOffice(t *testing.T) {
	data, err := os.ReadFile("testdata/libreoffice.xls")
	if err != nil {
		t.Fatal(err)
	}
	if sniffSheet(data) != sheetXLS {
		t.Fatal("not sniffed as xls")
	}
	rows, err := readSheet(data, "", false)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"name", "qty", "price", "total", "date", "share", "ok", "label"},
		{"Çay ☕ şeker", "3", "1.25", "3.75", "2024-03-15", "12.5%", "1", "0"},
		{"big", "123456", "-0.5", "-61728", "1999-12-31", "", "0", "#DIV/0!"},
	}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("rows:\n got %q\nwant %q", rows, want)
	}
	raw, _ := readSheet(data, "", true)
	if raw[1][4] != "45366" || raw[1][5] != "0.125" {
		t.Fatalf("raw values: %q", raw[1])
	}
	if second, err := readSheet(data, "Second", false); err != nil || fmt.Sprint(second) != "[[k] [v2]]" {
		t.Fatalf("second sheet: %q %v", second, err)
	}
	if _, err := readSheet(data, "Nope", false); err == nil || !strings.Contains(err.Error(), `"Nope" not found`) {
		t.Fatalf("missing sheet: %v", err)
	}
	for _, bad := range [][]byte{data[:600], append([]byte{}, cfbSignature...), bytes.Repeat([]byte{0xD0, 0xCF, 0x11, 0xE0}, 200)} {
		if _, err := readSheet(bad, "", false); err == nil {
			t.Fatalf("corrupt file accepted (%d bytes)", len(bad))
		}
	}
}

// biffBook builds a workbook stream from sheet records for reader tests.
func biffBook(t *testing.T, globals func(w *biffWriter), sheet func(w *biffWriter)) []byte {
	t.Helper()
	w := &biffWriter{}
	bof := func(dt int) {
		w.rec(0x0809, bytes.Join([][]byte{u16b(0x0600), u16b(dt), u16b(0), u16b(0), u32b(0), u32b(6)}, nil))
	}
	bof(5)
	for i := 0; i < 16; i++ {
		w.rec(0x00E0, make([]byte, 20))
	}
	at := w.buf.Len()
	w.rec(0x0085, append(append(u32b(0), 0, 0), shortStr("S")...))
	if globals != nil {
		globals(w)
	}
	w.rec(0x000A, nil)
	pos := w.buf.Len()
	bof(0x10)
	sheet(w)
	w.rec(0x000A, nil)
	out := w.buf.Bytes()
	binary.LittleEndian.PutUint32(out[at+4:], uint32(pos))
	return writeCFB("Workbook", out)
}

func TestReadXLSRecords(t *testing.T) {
	head := func(r, c int) []byte { return bytes.Join([][]byte{u16b(r), u16b(c), u16b(15)}, nil) }
	num := func(v float64) []byte {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
		return b[:]
	}
	data := biffBook(t, func(w *biffWriter) {
		// SST with a rich, extended string continued in a CONTINUE record
		// whose character data restarts with a compressed (8-bit) flag.
		sst := append(u32b(2), u32b(2)...)
		sst = append(sst, 5, 0, 0x0C, 1, 0, 2, 0, 0, 0) // cch=5, rich+ext, 1 run, ext 2 bytes
		sst = append(sst, 'a', 'b')
		w.rec(0x00FC, sst)
		cont := []byte{0x00, 'c', 'd', 'e', 0, 0, 0, 0, 9, 9} // 8-bit chars, run, ext
		cont = append(cont, 2, 0, 1, 0x3B, 0x01, 'X', 0)      // "ĻX" in UTF-16
		w.rec(0x003C, cont)
	}, func(w *biffWriter) {
		w.rec(0x00FD, append(head(0, 0), u32b(0)...))
		w.rec(0x00FD, append(head(0, 1), u32b(1)...))
		// Formula with a cached string result in the following STRING record.
		f := append(head(1, 0), 0, 0, 0, 0, 0, 0, 0xFF, 0xFF)
		w.rec(0x0006, append(f, make([]byte, 6)...))
		w.rec(0x0207, []byte{3, 0, 0, 'y', 'e', 's'})
		// Formula with a boolean result, an RK integer, an RK float /100.
		fb := append(head(1, 1), 1, 0, 1, 0, 0, 0, 0xFF, 0xFF)
		w.rec(0x0006, append(fb, make([]byte, 6)...))
		w.rec(0x027E, append(head(1, 2), u32b(42<<2|2)...))
		w.rec(0x027E, append(head(1, 3), u32b(1234<<2|3)...))
		// MULRK across three columns, a NUMBER and a LABEL.
		mul := append(u16b(2), u16b(0)...)
		for _, v := range []int{1, 2, 3} {
			mul = append(mul, u16b(15)...)
			mul = append(mul, u32b(v<<2|2)...)
		}
		w.rec(0x00BD, append(mul, u16b(2)...))
		w.rec(0x0203, append(head(3, 0), num(-1.5)...))
		w.rec(0x0204, append(head(3, 1), 2, 0, 0, 'h', 'i'))
		// An embedded chart substream must not end the sheet.
		w.rec(0x0809, bytes.Join([][]byte{u16b(0x0600), u16b(0x20), u16b(0), u16b(0), u32b(0), u32b(6)}, nil))
		w.rec(0x000A, nil)
		w.rec(0x0205, append(head(4, 0), 1, 0))
	})
	rows, err := readSheet(data, "", false)
	if err != nil {
		t.Fatal(err)
	}
	want := `[["abcde" "ĻX"] ["yes" "TRUE" "42" "12.34"] ["1" "2" "3"] ["-1.5" "hi"] ["TRUE"]]`
	if got := fmt.Sprintf("%q", rows); got != want {
		t.Fatalf("rows:\n got %s\nwant %s", got, want)
	}
}

func TestWriteSpreadsheetsRoundTrip(t *testing.T) {
	long := strings.Repeat("ğ", 9000) + "END" // spans CONTINUE records
	items := []Item{
		{JSON: map[string]interface{}{"name": "Çay ☕", "n": 3.0, "ok": true, "note": "  a  b\tc\nd"}},
		{JSON: map[string]interface{}{"name": long, "n": -0.5, "ok": false}},
		{JSON: map[string]interface{}{"name": "{x} \\", "n": 1e20, "obj": map[string]interface{}{"a": 1.0}}},
	}
	for i := 0; i < 2000; i++ { // more than 128 EXTSST buckets of 8
		items = append(items, Item{JSON: map[string]interface{}{"name": fmt.Sprintf("u%d", i), "n": float64(i)}})
	}
	x, err := writeXLS(items, "Data", true)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readSheet(x, "Data", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(items)+1 || fmt.Sprint(rows[0]) != "[n name note ok obj]" || rows[1][1] != "Çay ☕" || rows[1][2] != "  a  b\tc\nd" ||
		rows[1][3] != "TRUE" || rows[2][1] != long || rows[3][0] != "100000000000000000000" || rows[3][4] != `{"a":1}` || rows[len(rows)-1][1] != "u1999" {
		t.Fatalf("xls round trip: %q %q %q long=%v", rows[0], rows[1], rows[3], rows[2][1] == long)
	}
	if _, err := writeXLS(make([]Item, 70000), "", false); err == nil {
		t.Fatal("xls row limit not enforced")
	}

	o, err := writeODS(items[:3], "Data", true)
	if err != nil {
		t.Fatal(err)
	}
	if sniffSheet(o) != sheetODS {
		t.Fatal("ods not sniffed")
	}
	rows, err = readSheet(o, "Data", false)
	if err != nil || len(rows) != 4 || rows[1][2] != "  a  b\tc\nd" || rows[2][1] != long || rows[1][3] != "TRUE" {
		t.Fatalf("ods round trip: %q %v", rows, err)
	}

	rows, err = readRTFTable(writeRTF(items[:3], true))
	if err != nil || len(rows) != 4 || rows[1][1] != "Çay ☕" || rows[1][2] != "a  b\tc\nd" || rows[2][1] != long || rows[3][1] != "{x} \\" {
		t.Fatalf("rtf round trip: %q %v", rows, err)
	}
	// Text outside tables becomes single-cell rows; control words are dropped.
	rows, _ = readRTFTable([]byte(`{\rtf1\ansi{\fonttbl{\f0 Arial;}}{\*\generator x;}\f0 Hello \b world\b0\par caf\'e9 \u8364?\par}`))
	if fmt.Sprintf("%q", rows) != `[["Hello world"] ["café €"]]` {
		t.Fatalf("rtf text: %q", rows)
	}
}

func TestSpreadsheetNodesNewFormats(t *testing.T) {
	h := newHarness(t, &Options{Timezone: "Europe/Istanbul"})
	seed := nd("Seed", "code", 2, map[string]interface{}{"jsCode": "return [{json:{name:'Ada', n: 1}}, {json:{name:'Linus', n: 2}}];"})
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil), seed,
		nd("To XLS", "convertToFile", 1.1, map[string]interface{}{"operation": "xls", "options": map[string]interface{}{"sheetName": "People"}}),
		nd("From XLS", "extractFromFile", 1, map[string]interface{}{"operation": "xls"}),
		nd("To ODS", "spreadsheetFile", 2, map[string]interface{}{"operation": "toFile", "fileFormat": "ods"}),
		nd("From ODS", "spreadsheetFile", 2, map[string]interface{}{"operation": "fromFile"}),
		nd("To RTF", "convertToFile", 1.1, map[string]interface{}{"operation": "rtf"}),
		nd("From RTF", "extractFromFile", 1, map[string]interface{}{"operation": "rtf"}),
		nd("ICS", "convertToFile", 1.1, map[string]interface{}{"operation": "iCal", "title": "={{ $json.name }} sync", "start": "2025-05-01T10:00:00", "end": "2025-05-01T11:30:00",
			"additionalFields": map[string]interface{}{"description": "a, b; c", "location": "Room 1", "uid": "={{ 'id-' + $json.n }}",
				"attendeesUi": map[string]interface{}{"values": []interface{}{map[string]interface{}{"name": "Bob", "email": "bob@example.com", "rsvp": true}}}}}),
	}, "Start->Seed", "Seed->To XLS", "To XLS->From XLS", "Seed->To ODS", "To ODS->From ODS", "Seed->To RTF", "To RTF->From RTF", "Seed->ICS")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("blocked: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	for _, n := range []string{"From XLS", "From ODS", "From RTF"} {
		got := itemsOf(t, steps[n], 0)
		if len(got) != 2 || got[1]["name"] != "Linus" || toString(got[1]["n"]) != "2" {
			t.Fatalf("%s: %v", n, got)
		}
	}
	out := decodeOutput(steps["ICS"].Output)[0]
	if len(out) != 2 {
		t.Fatalf("ics items: %d", len(out))
	}
	b, mt, name, err := binaryOf(out[1], "data")
	ics := string(b)
	for _, want := range []string{"BEGIN:VCALENDAR\r\n", "SUMMARY:Linus sync\r\n", "UID:id-2\r\n", "DTSTART:20250501T070000Z\r\n", "DTEND:20250501T083000Z\r\n",
		`DESCRIPTION:a\, b\; c`, "LOCATION:Room 1", "ATTENDEE;RSVP=TRUE;CN=Bob:mailto:bob@example.com", "END:VCALENDAR\r\n"} {
		if !strings.Contains(ics, want) {
			t.Fatalf("ics missing %q:\n%s", want, ics)
		}
	}
	if err != nil || mt != "text/calendar" || name != "event.ics" {
		t.Fatalf("ics binary: %q %q %v", mt, name, err)
	}
}
