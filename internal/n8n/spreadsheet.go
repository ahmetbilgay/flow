package n8n

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// Spreadsheet limits guard against decompression bombs and huge sheets.
const (
	maxSheetRows = 100000
	maxSheetCols = 1000
)

type sheetKind int

const (
	sheetUnknown sheetKind = iota
	sheetXLSX
	sheetODS
	sheetXLS
)

// sniffSheet identifies a spreadsheet by content, like n8n's SheetJS does
// (an "xls" operation often receives an .xlsx file).
func sniffSheet(data []byte) sheetKind {
	switch {
	case bytes.HasPrefix(data, []byte{0xD0, 0xCF, 0x11, 0xE0}):
		return sheetXLS
	case bytes.HasPrefix(data, []byte("PK\x03\x04")):
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return sheetUnknown
		}
		for _, f := range zr.File {
			switch f.Name {
			case "xl/workbook.xml":
				return sheetXLSX
			case "content.xml":
				return sheetODS
			}
		}
	}
	return sheetUnknown
}

// readSheet returns the rows of a sheet (the first one by default) as
// displayed text, or raw values when raw is set.
func readSheet(data []byte, sheet string, raw bool) ([][]string, error) {
	switch sniffSheet(data) {
	case sheetXLSX:
		f, err := excelize.OpenReader(bytes.NewReader(data), excelize.Options{UnzipSizeLimit: maxDecompressedBytes, UnzipXMLSizeLimit: maxDecompressedBytes / 4})
		if err != nil {
			return nil, permanentError{"invalid xlsx file: " + err.Error()}
		}
		defer func() { _ = f.Close() }()
		if sheet == "" {
			sheet = f.GetSheetName(0)
		}
		rows, err := f.Rows(sheet)
		if err != nil {
			return nil, permanentError{fmt.Sprintf("sheet %q: %v", sheet, err)}
		}
		defer func() { _ = rows.Close() }()
		var out [][]string
		for rows.Next() {
			if len(out) >= maxSheetRows {
				return nil, permanentError{fmt.Sprintf("sheet has more than %d rows", maxSheetRows)}
			}
			cols, err := rows.Columns(excelize.Options{RawCellValue: raw})
			if err != nil {
				return nil, err
			}
			if len(cols) > maxSheetCols {
				cols = cols[:maxSheetCols]
			}
			out = append(out, cols)
		}
		return out, rows.Error()
	case sheetODS:
		return readODS(data, sheet)
	case sheetXLS:
		return readXLS(data, sheet, raw)
	}
	return nil, permanentError{"file is not an xlsx, xls or ods spreadsheet"}
}

// sheetItems turns rows into items like n8n's sheet_to_json: the first row
// names the fields (unless headerRow is false), empty cells are omitted
// unless includeEmpty, and blank rows are skipped.
func sheetItems(rows [][]string, header, includeEmpty bool) []Item {
	var cols []string
	var out []Item
	for ri, row := range rows {
		if header && ri == 0 {
			cols = row
			continue
		}
		j := map[string]interface{}{}
		blank := true
		width := len(row)
		if includeEmpty && len(cols) > width {
			width = len(cols)
		}
		for c := 0; c < width; c++ {
			v := ""
			if c < len(row) {
				v = row[c]
			}
			if v != "" {
				blank = false
			} else if !includeEmpty {
				continue
			}
			name := fmt.Sprintf("field_%d", c+1)
			if header && c < len(cols) && cols[c] != "" {
				name = cols[c]
			}
			j[name] = v
		}
		if !blank {
			out = append(out, Item{JSON: j})
		}
	}
	return out
}

// writeXLSX renders items as a one-sheet workbook.
func writeXLSX(items []Item, sheet string, header bool) ([]byte, error) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	if sheet == "" {
		sheet = "Sheet"
	}
	if err := f.SetSheetName(f.GetSheetName(0), sheet); err != nil {
		return nil, permanentError{fmt.Sprintf("sheet name %q: %v", sheet, err)}
	}
	sw, err := f.NewStreamWriter(sheet)
	if err != nil {
		return nil, err
	}
	cols := columnsOf(items)
	row := 1
	write := func(vals []interface{}) error {
		cell, _ := excelize.CoordinatesToCellName(1, row)
		row++
		return sw.SetRow(cell, vals)
	}
	if header {
		vals := make([]interface{}, len(cols))
		for c, k := range cols {
			vals[c] = k
		}
		if err := write(vals); err != nil {
			return nil, err
		}
	}
	for _, it := range items {
		vals := make([]interface{}, len(cols))
		for c, k := range cols {
			switch v := it.JSON[k].(type) {
			case nil:
				vals[c] = nil
			case float64, bool, string:
				vals[c] = v
			default:
				vals[c] = cellString(v)
			}
		}
		if err := write(vals); err != nil {
			return nil, err
		}
	}
	if err := sw.Flush(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func attrNS(se xml.StartElement, space, name string) string {
	for _, a := range se.Attr {
		if a.Name.Space == space && a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// readODS reads a sheet of an OpenDocument spreadsheet as displayed text.
func readODS(data []byte, sheet string) ([][]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, permanentError{"invalid ods file"}
	}
	var content *zip.File
	for _, f := range zr.File {
		if f.Name == "content.xml" {
			content = f
		}
	}
	if content == nil {
		return nil, permanentError{"invalid ods file: no content.xml"}
	}
	rc, err := content.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	dec := xml.NewDecoder(io.LimitReader(rc, maxDecompressedBytes))
	const tableNS = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
	const textNS = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
	attr := func(se xml.StartElement, name string) string {
		for _, a := range se.Attr {
			if a.Name.Space == tableNS && a.Name.Local == name {
				return a.Value
			}
		}
		return ""
	}
	repeat := func(s string, max int) int {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return 1
		}
		if n > max {
			return max
		}
		return n
	}
	var (
		rows               [][]string
		row                []string
		cell               strings.Builder
		inTable, found     bool
		cellRepeat, rowRep int
		paras              int
	)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, permanentError{"invalid ods content: " + err.Error()}
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch {
			case t.Name.Space == tableNS && t.Name.Local == "table":
				if !found && (sheet == "" || attr(t, "name") == sheet) {
					inTable, found = true, true
				}
			case !inTable:
			case t.Name.Space == tableNS && t.Name.Local == "table-row":
				row, rowRep = nil, repeat(attr(t, "number-rows-repeated"), maxSheetRows)
			case t.Name.Space == tableNS && (t.Name.Local == "table-cell" || t.Name.Local == "covered-table-cell"):
				cell.Reset()
				paras = 0
				cellRepeat = repeat(attr(t, "number-columns-repeated"), maxSheetCols)
			case t.Name.Space == textNS && t.Name.Local == "p":
				if paras > 0 {
					cell.WriteByte('\n')
				}
				paras++
			case t.Name.Space == textNS && t.Name.Local == "s":
				n, err := strconv.Atoi(attrNS(t, textNS, "c"))
				if err != nil || n < 1 {
					n = 1
				}
				cell.WriteString(strings.Repeat(" ", min(n, 1000)))
			case t.Name.Space == textNS && t.Name.Local == "tab":
				cell.WriteByte('\t')
			case t.Name.Space == textNS && t.Name.Local == "line-break":
				cell.WriteByte('\n')
			}
		case xml.CharData:
			if inTable && paras > 0 {
				cell.Write(t)
			}
		case xml.EndElement:
			if !inTable {
				continue
			}
			switch {
			case t.Name.Space == tableNS && t.Name.Local == "table":
				inTable = false
			case t.Name.Space == tableNS && (t.Name.Local == "table-cell" || t.Name.Local == "covered-table-cell"):
				for k := 0; k < cellRepeat && len(row) < maxSheetCols; k++ {
					row = append(row, cell.String())
				}
			case t.Name.Space == tableNS && t.Name.Local == "table-row":
				for len(row) > 0 && row[len(row)-1] == "" {
					row = row[:len(row)-1] // trailing filler cells
				}
				if len(row) == 0 {
					rowRep = 1 // repeated blank filler rows
				}
				for k := 0; k < rowRep; k++ {
					if len(rows) >= maxSheetRows {
						return nil, permanentError{fmt.Sprintf("sheet has more than %d rows", maxSheetRows)}
					}
					rows = append(rows, row)
				}
			}
		}
	}
	if !found {
		return nil, permanentError{fmt.Sprintf("sheet %q not found", sheet)}
	}
	for len(rows) > 0 && len(rows[len(rows)-1]) == 0 {
		rows = rows[:len(rows)-1]
	}
	return rows, nil
}
