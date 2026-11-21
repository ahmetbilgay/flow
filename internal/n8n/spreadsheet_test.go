package n8n

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
)

func TestXLSXRoundTrip(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Rows", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":[{"name":"Ada","qty":3,"ok":true},{"name":"Bob","qty":4.5},{"name":"","qty":null}]}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		nd("To XLSX", "convertToFile", 1.1, map[string]interface{}{"operation": "xlsx", "options": map[string]interface{}{"sheetName": "Data"}}),
		nd("Read", "extractFromFile", 1, map[string]interface{}{"operation": "xls"}), // an "xls" op receiving .xlsx, as in templates
		nd("Raw", "extractFromFile", 1, map[string]interface{}{"operation": "xlsx", "options": map[string]interface{}{"rawData": true, "sheetName": "Data"}}),
		nd("Sheet out", "spreadsheetFile", 1, map[string]interface{}{"operation": "toFile", "fileFormat": "xlsx"}),
		nd("Sheet in", "spreadsheetFile", 1, map[string]interface{}{}),
	}, "Start->Rows", "Rows->Split", "Split->To XLSX", "To XLSX->Read", "To XLSX->Raw", "Split->Sheet out", "Sheet out->Sheet in")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	bin := decodeOutput(steps["To XLSX"].Output)[0][0]
	if b := bin.Binary["data"].(map[string]interface{}); b["fileName"] != "data.xlsx" || !strings.Contains(toString(b["mimeType"]), "spreadsheetml") {
		t.Fatalf("xlsx binary: %v", b)
	}
	got := itemsOf(t, steps["Read"], 0)
	if len(got) != 2 || got[0]["name"] != "Ada" || got[0]["qty"] != "3" || got[0]["ok"] != "TRUE" || got[1]["qty"] != "4.5" {
		t.Fatalf("read (blank row skipped, displayed values): %v", got)
	}
	if raw := itemsOf(t, steps["Raw"], 0); len(raw) != 2 || raw[0]["ok"] != "1" {
		t.Fatalf("raw: %v", raw)
	}
	if in := itemsOf(t, steps["Sheet in"], 0); len(in) != 2 || in[1]["name"] != "Bob" {
		t.Fatalf("spreadsheet file round trip: %v", in)
	}
}

func odsFile(t *testing.T, content string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("mimetype")
	_, _ = w.Write([]byte("application/vnd.oasis.opendocument.spreadsheet"))
	w, _ = zw.Create("content.xml")
	_, _ = w.Write([]byte(`<?xml version="1.0"?><office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"><office:body><office:spreadsheet>` + content + `</office:spreadsheet></office:body></office:document-content>`))
	_ = zw.Close()
	return buf.Bytes()
}

func TestReadODS(t *testing.T) {
	ods := odsFile(t, `<table:table table:name="Other"><table:table-row><table:table-cell><text:p>x</text:p></table:table-cell></table:table-row></table:table>`+
		`<table:table table:name="People">`+
		`<table:table-row><table:table-cell><text:p>name</text:p></table:table-cell><table:table-cell><text:p>city</text:p></table:table-cell><table:table-cell><text:p>note</text:p></table:table-cell></table:table-row>`+
		`<table:table-row><table:table-cell><text:p>Ada</text:p></table:table-cell><table:table-cell table:number-columns-repeated="2"><text:p>Paris</text:p></table:table-cell><table:table-cell table:number-columns-repeated="16000"/></table:table-row>`+
		`<table:table-row table:number-rows-repeated="1000000"><table:table-cell table:number-columns-repeated="1024"/></table:table-row>`+
		`<table:table-row><table:table-cell><text:p>Bob<text:s/>Jr</text:p><text:p>II</text:p></table:table-cell></table:table-row>`+
		`</table:table>`)
	rows, err := readSheet(ods, "People", false)
	if err != nil {
		t.Fatal(err)
	}
	items := sheetItems(rows, true, false)
	if len(items) != 2 || items[0].JSON["city"] != "Paris" || items[0].JSON["note"] != "Paris" || items[1].JSON["name"] != "Bob Jr\nII" {
		t.Fatalf("ods items: %v", items)
	}
	if rows, _ := readSheet(ods, "", false); len(rows) != 1 || rows[0][0] != "x" {
		t.Fatalf("default sheet is the first: %v", rows)
	}
	if _, err := readSheet([]byte{0xD0, 0xCF, 0x11, 0xE0, 0, 0}, "", false); err == nil || !strings.Contains(err.Error(), "invalid xls file") {
		t.Fatalf("truncated xls: %v", err)
	}
}
