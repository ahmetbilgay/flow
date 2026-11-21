package n8n

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

// Legacy Excel 97-2003 (.xls) workbooks: a BIFF8 "Workbook" stream inside
// a Compound File Binary (CFB) container. readXLS also reads BIFF5/7
// (Excel 5/95) "Book" streams; writeXLS produces BIFF8.

// ---------------------------------------------------------------- CFB

const (
	cfbFree       = 0xFFFFFFFF
	cfbEndOfChain = 0xFFFFFFFE
	cfbFATSect    = 0xFFFFFFFD
	cfbDIFATSect  = 0xFFFFFFFC
	cfbNoStream   = 0xFFFFFFFF
)

var cfbSignature = []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}

type cfbFile struct {
	data      []byte
	secSize   int
	fat       []uint32
	miniFAT   []uint32
	miniData  []byte
	miniLimit uint32
	entries   []cfbEntry
}

type cfbEntry struct {
	name  string
	typ   byte
	start uint32
	size  uint64
}

func xlsErr(format string, a ...interface{}) error {
	return permanentError{"invalid xls file: " + fmt.Sprintf(format, a...)}
}

func openCFB(data []byte) (*cfbFile, error) {
	if len(data) < 512 || !bytes.Equal(data[:8], cfbSignature) {
		return nil, xlsErr("not a compound document")
	}
	le := binary.LittleEndian
	shift := le.Uint16(data[0x1E:])
	if shift != 9 && shift != 12 {
		return nil, xlsErr("sector size")
	}
	f := &cfbFile{data: data, secSize: 1 << shift, miniLimit: le.Uint32(data[0x38:])}
	maxSectors := uint32(len(data) / f.secSize)
	sector := func(n uint32) []byte {
		off := (int(n) + 1) * f.secSize
		if n >= maxSectors || off+f.secSize > len(data) {
			return nil
		}
		return data[off : off+f.secSize]
	}
	// FAT sectors: 109 in the header, the rest in the DIFAT chain.
	var fatSecs []uint32
	for i := 0; i < 109; i++ {
		if s := le.Uint32(data[0x4C+4*i:]); s < cfbDIFATSect {
			fatSecs = append(fatSecs, s)
		}
	}
	perSec := f.secSize / 4
	for d, n := le.Uint32(data[0x44:]), 0; d < cfbDIFATSect && n < int(maxSectors); n++ {
		b := sector(d)
		if b == nil {
			return nil, xlsErr("DIFAT")
		}
		for i := 0; i < perSec-1; i++ {
			if s := le.Uint32(b[4*i:]); s < cfbDIFATSect {
				fatSecs = append(fatSecs, s)
			}
		}
		d = le.Uint32(b[4*(perSec-1):])
	}
	for _, s := range fatSecs {
		b := sector(s)
		if b == nil {
			return nil, xlsErr("FAT")
		}
		for i := 0; i < perSec; i++ {
			f.fat = append(f.fat, le.Uint32(b[4*i:]))
		}
	}
	dir, err := f.chain(le.Uint32(data[0x30:]), f.fat, sector, -1)
	if err != nil {
		return nil, err
	}
	for off := 0; off+128 <= len(dir); off += 128 {
		e := dir[off : off+128]
		nl := int(le.Uint16(e[64:]))
		if nl > 64 || nl < 2 {
			f.entries = append(f.entries, cfbEntry{})
			continue
		}
		u := make([]uint16, nl/2-1)
		for i := range u {
			u[i] = le.Uint16(e[2*i:])
		}
		size := le.Uint64(e[120:])
		if f.secSize == 512 {
			size &= 0xFFFFFFFF
		}
		f.entries = append(f.entries, cfbEntry{name: string(utf16.Decode(u)), typ: e[66], start: le.Uint32(e[116:]), size: size})
	}
	if len(f.entries) == 0 || f.entries[0].typ != 5 {
		return nil, xlsErr("no root entry")
	}
	if mf := le.Uint32(data[0x3C:]); mf < cfbDIFATSect {
		b, err := f.chain(mf, f.fat, sector, -1)
		if err != nil {
			return nil, err
		}
		for i := 0; i+4 <= len(b); i += 4 {
			f.miniFAT = append(f.miniFAT, le.Uint32(b[i:]))
		}
		root := f.entries[0]
		if f.miniData, err = f.chain(root.start, f.fat, sector, int(root.size)); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// chain concatenates a sector chain; size < 0 reads it whole.
func (f *cfbFile) chain(start uint32, fat []uint32, sector func(uint32) []byte, size int) ([]byte, error) {
	var out []byte
	for s, n := start, 0; s < cfbDIFATSect; n++ {
		if n > len(fat) || int(s) >= len(fat) {
			return nil, xlsErr("broken sector chain")
		}
		b := sector(s)
		if b == nil {
			return nil, xlsErr("sector out of range")
		}
		out = append(out, b...)
		if size >= 0 && len(out) >= size {
			break
		}
		s = fat[s]
	}
	if size >= 0 {
		if len(out) < size {
			return nil, xlsErr("truncated stream")
		}
		out = out[:size]
	}
	return out, nil
}

func (f *cfbFile) stream(name string) ([]byte, bool, error) {
	for _, e := range f.entries {
		if e.typ != 2 || !strings.EqualFold(e.name, name) {
			continue
		}
		if e.size > uint64(len(f.data))*2 {
			return nil, true, xlsErr("stream size")
		}
		if e.size < uint64(f.miniLimit) {
			const mini = 64
			sector := func(n uint32) []byte {
				off := int(n) * mini
				if off+mini > len(f.miniData) {
					return nil
				}
				return f.miniData[off : off+mini]
			}
			b, err := f.chain(e.start, f.miniFAT, sector, int(e.size))
			return b, true, err
		}
		maxSectors := uint32(len(f.data) / f.secSize)
		sector := func(n uint32) []byte {
			off := (int(n) + 1) * f.secSize
			if n >= maxSectors || off+f.secSize > len(f.data) {
				return nil
			}
			return f.data[off : off+f.secSize]
		}
		b, err := f.chain(e.start, f.fat, sector, int(e.size))
		return b, true, err
	}
	return nil, false, nil
}

// writeCFB wraps one stream in a version 3 compound document.
func writeCFB(name string, stream []byte) []byte {
	const sec = 512
	if len(stream) < 4096 {
		// Streams below the mini-stream cutoff would need a mini FAT.
		stream = append(stream, make([]byte, 4096-len(stream))...)
	}
	streamSecs := (len(stream) + sec - 1) / sec
	fatSecs, difatSecs := 0, 0
	for {
		total := streamSecs + 1 + fatSecs + difatSecs
		needFAT := (total + 127) / 128
		needDIFAT := 0
		if needFAT > 109 {
			needDIFAT = (needFAT - 109 + 126) / 127
		}
		if needFAT == fatSecs && needDIFAT == difatSecs {
			break
		}
		fatSecs, difatSecs = needFAT, needDIFAT
	}
	firstDIFAT := uint32(fatSecs)
	dirSec := uint32(fatSecs + difatSecs)
	firstStream := dirSec + 1
	le := binary.LittleEndian
	fat := make([]uint32, fatSecs*128)
	for i := range fat {
		fat[i] = cfbFree
	}
	for i := 0; i < fatSecs; i++ {
		fat[i] = cfbFATSect
	}
	for i := 0; i < difatSecs; i++ {
		fat[fatSecs+i] = cfbDIFATSect
	}
	fat[dirSec] = cfbEndOfChain
	for i := 0; i < streamSecs; i++ {
		fat[int(firstStream)+i] = firstStream + uint32(i) + 1
	}
	fat[int(firstStream)+streamSecs-1] = cfbEndOfChain

	out := make([]byte, sec*(1+int(firstStream)+streamSecs))
	h := out[:sec]
	copy(h, cfbSignature)
	le.PutUint16(h[0x18:], 0x003E)
	le.PutUint16(h[0x1A:], 3)
	le.PutUint16(h[0x1C:], 0xFFFE)
	le.PutUint16(h[0x1E:], 9)
	le.PutUint16(h[0x20:], 6)
	le.PutUint32(h[0x2C:], uint32(fatSecs))
	le.PutUint32(h[0x30:], dirSec)
	le.PutUint32(h[0x38:], 4096)
	le.PutUint32(h[0x3C:], cfbEndOfChain)
	if difatSecs > 0 {
		le.PutUint32(h[0x44:], firstDIFAT)
	} else {
		le.PutUint32(h[0x44:], cfbEndOfChain)
	}
	le.PutUint32(h[0x48:], uint32(difatSecs))
	for i := 0; i < 109; i++ {
		v := uint32(cfbFree)
		if i < fatSecs {
			v = uint32(i)
		}
		le.PutUint32(h[0x4C+4*i:], v)
	}
	at := func(n uint32) []byte { return out[sec*(int(n)+1) : sec*(int(n)+2)] }
	for i := 0; i < fatSecs; i++ {
		b := at(uint32(i))
		for k := 0; k < 128; k++ {
			le.PutUint32(b[4*k:], fat[i*128+k])
		}
	}
	for d := 0; d < difatSecs; d++ {
		b := at(firstDIFAT + uint32(d))
		for k := 0; k < 127; k++ {
			v := uint32(cfbFree)
			if idx := 109 + d*127 + k; idx < fatSecs {
				v = uint32(idx)
			}
			le.PutUint32(b[4*k:], v)
		}
		next := uint32(cfbEndOfChain)
		if d < difatSecs-1 {
			next = firstDIFAT + uint32(d) + 1
		}
		le.PutUint32(b[4*127:], next)
	}
	dir := at(dirSec)
	entry := func(i int, n string, typ byte, child, start uint32, size uint32) {
		e := dir[128*i : 128*(i+1)]
		u := utf16.Encode([]rune(n))
		for k, c := range u {
			le.PutUint16(e[2*k:], c)
		}
		le.PutUint16(e[64:], uint16(2*(len(u)+1)))
		e[66], e[67] = typ, 1
		le.PutUint32(e[68:], cfbNoStream)
		le.PutUint32(e[72:], cfbNoStream)
		le.PutUint32(e[76:], child)
		le.PutUint32(e[116:], start)
		le.PutUint32(e[120:], size)
	}
	entry(0, "Root Entry", 5, 1, cfbEndOfChain, 0)
	entry(1, name, 2, cfbNoStream, firstStream, uint32(len(stream)))
	for i := 2; i < 4; i++ {
		e := dir[128*i:]
		le.PutUint32(e[68:], cfbNoStream)
		le.PutUint32(e[72:], cfbNoStream)
		le.PutUint32(e[76:], cfbNoStream)
	}
	copy(out[sec*(int(firstStream)+1):], stream)
	return out
}

// ---------------------------------------------------------------- BIFF read

type biffRec struct {
	typ  uint16
	data []byte
	pos  int // stream offset of the record header
}

func biffRecords(s []byte) []biffRec {
	var out []biffRec
	for off := 0; off+4 <= len(s); {
		typ := binary.LittleEndian.Uint16(s[off:])
		n := int(binary.LittleEndian.Uint16(s[off+2:]))
		if off+4+n > len(s) {
			break
		}
		out = append(out, biffRec{typ, s[off+4 : off+4+n], off})
		off += 4 + n
	}
	return out
}

// biffStr reads strings that may continue across CONTINUE records, where
// each continuation of character data restarts with an encoding flag.
type biffStr struct {
	segs [][]byte
	seg  int
	off  int
	err  bool
}

func (r *biffStr) avail() int {
	if r.seg >= len(r.segs) {
		return 0
	}
	return len(r.segs[r.seg]) - r.off
}

func (r *biffStr) bytes(n int) []byte {
	var out []byte
	for n > 0 {
		if r.avail() == 0 {
			r.seg, r.off = r.seg+1, 0
			if r.seg >= len(r.segs) {
				r.err = true
				return out
			}
			continue
		}
		k := min(n, r.avail())
		out = append(out, r.segs[r.seg][r.off:r.off+k]...)
		r.off += k
		n -= k
	}
	return out
}

func (r *biffStr) u8() byte {
	b := r.bytes(1)
	if len(b) < 1 {
		return 0
	}
	return b[0]
}

func (r *biffStr) u16() int {
	b := r.bytes(2)
	if len(b) < 2 {
		return 0
	}
	return int(binary.LittleEndian.Uint16(b))
}

func (r *biffStr) u32() int {
	b := r.bytes(4)
	if len(b) < 4 {
		return 0
	}
	return int(int32(binary.LittleEndian.Uint32(b)))
}

// chars reads cch characters, honoring encoding flags at segment starts.
func (r *biffStr) chars(cch int, wide bool) string {
	var u []uint16
	for cch > 0 && !r.err {
		if r.avail() == 0 {
			r.seg, r.off = r.seg+1, 0
			if r.seg >= len(r.segs) {
				r.err = true
				break
			}
			wide = r.u8()&1 != 0
			continue
		}
		if wide {
			if r.avail() < 2 {
				r.err = true
				break
			}
			u = append(u, binary.LittleEndian.Uint16(r.segs[r.seg][r.off:]))
			r.off += 2
		} else {
			u = append(u, uint16(r.segs[r.seg][r.off]))
			r.off++
		}
		cch--
	}
	return string(utf16.Decode(u))
}

// unicodeStr reads an XLUnicodeRichExtendedString (lenBytes 2) or a
// ShortXLUnicodeString (lenBytes 1).
func (r *biffStr) unicodeStr(lenBytes int) string {
	cch := int(r.u8())
	if lenBytes == 2 {
		cch |= int(r.u8()) << 8
	}
	flags := r.u8()
	runs, ext := 0, 0
	if flags&0x08 != 0 {
		runs = r.u16()
	}
	if flags&0x04 != 0 {
		ext = r.u32()
	}
	s := r.chars(cch, flags&1 != 0)
	if runs > 0 {
		r.bytes(4 * runs)
	}
	if ext > 0 {
		r.bytes(ext)
	}
	return s
}

func oneSeg(b []byte) *biffStr { return &biffStr{segs: [][]byte{b}} }

// latin1 decodes BIFF5 byte strings (codepage 1252 approximated as Latin-1).
func latin1(b []byte) string {
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}

type xlsBook struct {
	biff8    bool
	sst      []string
	xfFmt    []int
	formats  map[int]string
	date1904 bool
	sheets   []xlsSheetRef
}

type xlsSheetRef struct {
	name string
	pos  int
	kind byte
}

func readXLS(data []byte, sheet string, raw bool) ([][]string, error) {
	cf, err := openCFB(data)
	if err != nil {
		return nil, err
	}
	stream, ok, err := cf.stream("Workbook")
	if err == nil && !ok {
		stream, ok, err = cf.stream("Book")
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, xlsErr("no Workbook stream (is it encrypted or not an Excel file?)")
	}
	recs := biffRecords(stream)
	if len(recs) == 0 || recs[0].typ != 0x0809 && recs[0].typ != 0x0409 && recs[0].typ != 0x0209 {
		return nil, xlsErr("no BOF record")
	}
	bk := &xlsBook{formats: map[int]string{}}
	if len(recs[0].data) >= 2 {
		bk.biff8 = binary.LittleEndian.Uint16(recs[0].data) == 0x0600
	}
	if !bk.biff8 && recs[0].typ != 0x0809 {
		return nil, permanentError{"xls files older than Excel 5 are not supported"}
	}
	le := binary.LittleEndian
	for i := 1; i < len(recs); i++ {
		r := recs[i]
		d := r.data
		switch r.typ {
		case 0x000A: // EOF of the globals
			i = len(recs)
		case 0x002F: // FILEPASS
			return nil, permanentError{"password-protected xls files are not supported"}
		case 0x0022: // DATE1904
			bk.date1904 = len(d) >= 2 && le.Uint16(d) == 1
		case 0x041E, 0x001E: // FORMAT
			if len(d) < 3 {
				continue
			}
			id := int(le.Uint16(d))
			if bk.biff8 {
				bk.formats[id] = oneSeg(d[2:]).unicodeStr(2)
			} else if n := int(d[2]); 3+n <= len(d) {
				bk.formats[id] = latin1(d[3 : 3+n])
			}
		case 0x00E0: // XF
			if len(d) >= 4 {
				bk.xfFmt = append(bk.xfFmt, int(le.Uint16(d[2:])))
			}
		case 0x0085: // BOUNDSHEET
			if len(d) < 7 {
				continue
			}
			ref := xlsSheetRef{pos: int(le.Uint32(d)), kind: d[5]}
			if bk.biff8 {
				ref.name = oneSeg(d[6:]).unicodeStr(1)
			} else if n := int(d[6]); 7+n <= len(d) {
				ref.name = latin1(d[7 : 7+n])
			}
			bk.sheets = append(bk.sheets, ref)
		case 0x00FC: // SST
			segs := [][]byte{d}
			for i+1 < len(recs) && recs[i+1].typ == 0x003C {
				i++
				segs = append(segs, recs[i].data)
			}
			if len(d) < 8 {
				continue
			}
			rd := &biffStr{segs: segs, off: 8}
			n := int(le.Uint32(d[4:]))
			for k := 0; k < n && !rd.err; k++ {
				bk.sst = append(bk.sst, rd.unicodeStr(2))
			}
		}
	}
	var target *xlsSheetRef
	for k := range bk.sheets {
		s := &bk.sheets[k]
		if s.kind != 0 {
			continue // chart, macro and VB sheets
		}
		if sheet == "" || s.name == sheet {
			target = s
			break
		}
	}
	if target == nil {
		if sheet == "" {
			return nil, xlsErr("no worksheet")
		}
		return nil, permanentError{fmt.Sprintf("sheet %q not found", sheet)}
	}
	if target.pos < 0 || target.pos >= len(stream) {
		return nil, xlsErr("sheet offset")
	}
	return bk.readSheet(biffRecords(stream[target.pos:]), raw)
}

func (bk *xlsBook) readSheet(recs []biffRec, raw bool) ([][]string, error) {
	le := binary.LittleEndian
	var rows [][]string
	set := func(row, col int, v string) error {
		if row >= maxSheetRows {
			return permanentError{fmt.Sprintf("sheet has more than %d rows", maxSheetRows)}
		}
		if col >= maxSheetCols || v == "" {
			return nil
		}
		for len(rows) <= row {
			rows = append(rows, nil)
		}
		for len(rows[row]) <= col {
			rows[row] = append(rows[row], "")
		}
		rows[row][col] = v
		return nil
	}
	num := func(xf int, v float64) string { return bk.formatNumber(xf, v, raw) }
	depth := 0
	for i := 0; i < len(recs); i++ {
		r := recs[i]
		d := r.data
		// Embedded charts are nested BOF…EOF substreams.
		switch r.typ {
		case 0x0809:
			depth++
			continue
		case 0x000A:
			depth--
			if depth <= 0 {
				i = len(recs)
			}
			continue
		}
		if depth != 1 {
			continue
		}
		var err error
		switch r.typ {
		case 0x00FD: // LABELSST
			if len(d) >= 10 {
				if k := int(le.Uint32(d[6:])); k < len(bk.sst) {
					err = set(int(le.Uint16(d)), int(le.Uint16(d[2:])), bk.sst[k])
				}
			}
		case 0x0204, 0x00D6: // LABEL, RSTRING
			if len(d) < 8 {
				continue
			}
			var s string
			if bk.biff8 {
				s = oneSeg(d[6:]).unicodeStr(2)
			} else if n := int(le.Uint16(d[6:])); 8+n <= len(d) {
				s = latin1(d[8 : 8+n])
			}
			err = set(int(le.Uint16(d)), int(le.Uint16(d[2:])), s)
		case 0x0203: // NUMBER
			if len(d) >= 14 {
				err = set(int(le.Uint16(d)), int(le.Uint16(d[2:])), num(int(le.Uint16(d[4:])), math.Float64frombits(le.Uint64(d[6:]))))
			}
		case 0x027E: // RK
			if len(d) >= 10 {
				err = set(int(le.Uint16(d)), int(le.Uint16(d[2:])), num(int(le.Uint16(d[4:])), rkValue(le.Uint32(d[6:]))))
			}
		case 0x00BD: // MULRK
			if len(d) < 6 {
				continue
			}
			row, col := int(le.Uint16(d)), int(le.Uint16(d[2:]))
			for off := 4; off+6 <= len(d)-2 && err == nil; off += 6 {
				err = set(row, col, num(int(le.Uint16(d[off:])), rkValue(le.Uint32(d[off+2:]))))
				col++
			}
		case 0x0205: // BOOLERR
			if len(d) >= 8 {
				v := xlsErrorText(d[6])
				if d[7] == 0 {
					v = map[bool]string{true: "TRUE", false: "FALSE"}[d[6] != 0]
				}
				err = set(int(le.Uint16(d)), int(le.Uint16(d[2:])), v)
			}
		case 0x0006: // FORMULA: the cached result
			if len(d) < 14 {
				continue
			}
			row, col, xf := int(le.Uint16(d)), int(le.Uint16(d[2:])), int(le.Uint16(d[4:]))
			res := d[6:14]
			if le.Uint16(res[6:]) != 0xFFFF {
				if err := set(row, col, num(xf, math.Float64frombits(le.Uint64(res)))); err != nil {
					return nil, err
				}
				continue
			}
			switch res[0] {
			case 0: // string in the following STRING record
				for i+1 < len(recs) && (recs[i+1].typ == 0x04BC || recs[i+1].typ == 0x0221 || recs[i+1].typ == 0x0236) {
					i++ // SHRFMLA, ARRAY, TABLE
				}
				if i+1 < len(recs) && recs[i+1].typ == 0x0207 {
					i++
					sd := recs[i].data
					var s string
					if bk.biff8 {
						s = oneSeg(sd).unicodeStr(2)
					} else if len(sd) >= 2 {
						if n := int(le.Uint16(sd)); 2+n <= len(sd) {
							s = latin1(sd[2 : 2+n])
						}
					}
					err = set(row, col, s)
				}
			case 1:
				err = set(row, col, map[bool]string{true: "TRUE", false: "FALSE"}[res[2] != 0])
			case 2:
				err = set(row, col, xlsErrorText(res[2]))
			}
		}
		if err != nil {
			return nil, err
		}
	}
	for len(rows) > 0 && len(rows[len(rows)-1]) == 0 {
		rows = rows[:len(rows)-1]
	}
	return rows, nil
}

func rkValue(rk uint32) float64 {
	var v float64
	if rk&2 != 0 {
		v = float64(int32(rk) >> 2)
	} else {
		v = math.Float64frombits(uint64(rk&0xFFFFFFFC) << 32)
	}
	if rk&1 != 0 {
		v /= 100
	}
	return v
}

func xlsErrorText(code byte) string {
	switch code {
	case 0x00:
		return "#NULL!"
	case 0x07:
		return "#DIV/0!"
	case 0x0F:
		return "#VALUE!"
	case 0x17:
		return "#REF!"
	case 0x1D:
		return "#NAME?"
	case 0x24:
		return "#NUM!"
	case 0x2A:
		return "#N/A"
	}
	return "#ERR!"
}

// formatNumber renders a number cell: dates and percentages by their
// format, other numbers in general format; raw keeps the plain number.
func (bk *xlsBook) formatNumber(xf int, v float64, raw bool) string {
	plain := strconv.FormatFloat(v, 'f', -1, 64)
	if raw || xf < 0 || xf >= len(bk.xfFmt) {
		return plain
	}
	id := bk.xfFmt[xf]
	code, custom := bk.formats[id]
	switch {
	case id == 9 || id == 10 || custom && strings.Contains(code, "%"):
		dec := 0
		if id == 10 {
			dec = 2
		} else if i := strings.Index(code, "."); i >= 0 {
			for _, c := range code[i+1:] {
				if c != '0' {
					break
				}
				dec++
			}
		}
		return strconv.FormatFloat(v*100, 'f', dec, 64) + "%"
	case id >= 14 && id <= 22 || id >= 45 && id <= 47 || custom && isDateFormat(code):
		t := excelTime(v, bk.date1904)
		timeOnly := id >= 18 && id <= 21 || id >= 45 && id <= 47
		hasDate := !timeOnly && (!custom || strings.ContainsAny(strings.ToLower(stripFormatLiterals(code)), "dy"))
		hasTime := id >= 18 && id <= 22 || id >= 45 && id <= 47 || custom && strings.ContainsAny(strings.ToLower(stripFormatLiterals(code)), "hs")
		switch {
		case hasDate && hasTime:
			return t.Format("2006-01-02 15:04:05")
		case hasTime:
			return t.Format("15:04:05")
		default:
			return t.Format("2006-01-02")
		}
	}
	return plain
}

// stripFormatLiterals drops quoted text, escapes and [color]/[$-409]
// sections from a number format.
func stripFormatLiterals(code string) string {
	var b strings.Builder
	quoted, bracket := false, false
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case quoted:
			quoted = c != '"'
		case bracket:
			bracket = c != ']'
		case c == '"':
			quoted = true
		case c == '[':
			bracket = true
		case c == '\\' || c == '_' || c == '*':
			i++
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func isDateFormat(code string) bool {
	s := strings.ToLower(stripFormatLiterals(code))
	if s == "general" {
		return false
	}
	return strings.ContainsAny(s, "dmyhs") && !strings.ContainsAny(s, "#?")
}

func excelTime(v float64, date1904 bool) time.Time {
	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
	if date1904 {
		base = time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)
	} else if v < 61 {
		base = base.AddDate(0, 0, 1) // Excel's fictitious 1900-02-29
	}
	days := math.Floor(v)
	ms := math.Round((v - days) * 86400000)
	return base.AddDate(0, 0, int(days)).Add(time.Duration(ms) * time.Millisecond)
}

// ---------------------------------------------------------------- BIFF write

type biffWriter struct{ buf bytes.Buffer }

func (w *biffWriter) rec(typ uint16, data []byte) {
	var h [4]byte
	binary.LittleEndian.PutUint16(h[:], typ)
	binary.LittleEndian.PutUint16(h[2:], uint16(len(data)))
	w.buf.Write(h[:])
	w.buf.Write(data)
}

func u16b(v int) []byte { return []byte{byte(v), byte(v >> 8)} }
func u32b(v int) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }

// shortStr is a ShortXLUnicodeString in UTF-16.
func shortStr(s string) []byte {
	u := utf16.Encode([]rune(s))
	if len(u) > 255 {
		u = u[:255]
	}
	out := []byte{byte(len(u)), 1}
	for _, c := range u {
		out = append(out, byte(c), byte(c>>8))
	}
	return out
}

const biffMaxData = 8224

// writeXLS renders items as a one-sheet BIFF8 workbook.
func writeXLS(items []Item, sheet string, header bool) ([]byte, error) {
	if sheet == "" {
		sheet = "Sheet"
	}
	if len(sheet) > 31 || strings.ContainsAny(sheet, `[]:*?/\`) {
		return nil, permanentError{fmt.Sprintf("sheet name %q is not valid for xls", sheet)}
	}
	cols := columnsOf(items)
	nrows := len(items)
	if header {
		nrows++
	}
	if nrows > 65536 || len(cols) > 256 {
		return nil, permanentError{fmt.Sprintf("xls sheets hold at most 65536 rows and 256 columns (got %d × %d); use xlsx", nrows, len(cols))}
	}
	// Cell values; strings go to the shared string table.
	type cell struct {
		kind byte // 's' string index, 'n' number, 'b' bool
		s    int
		n    float64
		b    bool
	}
	sstIndex := map[string]int{}
	var sst []string
	str := func(s string) cell {
		if k, ok := sstIndex[s]; ok {
			return cell{kind: 's', s: k}
		}
		sstIndex[s] = len(sst)
		sst = append(sst, s)
		return cell{kind: 's', s: len(sst) - 1}
	}
	grid := make([][]*cell, 0, nrows)
	total := 0
	if header {
		row := make([]*cell, len(cols))
		for c, k := range cols {
			v := str(k)
			row[c] = &v
			total++
		}
		grid = append(grid, row)
	}
	for _, it := range items {
		row := make([]*cell, len(cols))
		for c, k := range cols {
			var v cell
			switch x := it.JSON[k].(type) {
			case nil:
				continue
			case float64:
				if math.IsInf(x, 0) || math.IsNaN(x) {
					v = str(cellString(x))
				} else {
					v = cell{kind: 'n', n: x}
				}
			case bool:
				v = cell{kind: 'b', b: x}
			case string:
				if x == "" {
					continue
				}
				if len(utf16.Encode([]rune(x))) > 32767 {
					return nil, permanentError{"xls cells hold at most 32767 characters; use xlsx"}
				}
				v = str(x)
			default:
				v = str(cellString(x))
			}
			row[c] = &v
			total++
		}
		grid = append(grid, row)
	}

	w := &biffWriter{}
	bof := func(dt int) {
		w.rec(0x0809, bytes.Join([][]byte{u16b(0x0600), u16b(dt), u16b(0x0DBB), u16b(0x07CC), u32b(0), u32b(6)}, nil))
	}
	bof(0x0005)
	w.rec(0x00E1, u16b(0x04B0))                                                                                                                   // INTERFACEHDR
	w.rec(0x00C1, u16b(0))                                                                                                                        // MMS
	w.rec(0x00E2, nil)                                                                                                                            // INTERFACEEND
	w.rec(0x0042, u16b(0x04B0))                                                                                                                   // CODEPAGE: UTF-16
	w.rec(0x003D, bytes.Join([][]byte{u16b(0), u16b(0), u16b(0x4000), u16b(0x2000), u16b(0x0038), u16b(0), u16b(0), u16b(1), u16b(0x0258)}, nil)) // WINDOW1
	w.rec(0x0022, u16b(0))                                                                                                                        // DATE1904
	font := bytes.Join([][]byte{u16b(200), u16b(0), u16b(0x7FFF), u16b(400), u16b(0), {0, 0, 0, 0}, shortStr("Arial")}, nil)
	for i := 0; i < 4; i++ {
		w.rec(0x0031, font) // FONT 0-3 (index 4 does not exist in BIFF)
	}
	styleXF := []byte{0, 0, 0, 0, 0xF5, 0xFF, 0x20, 0, 0, 0xF4, 0, 0, 0, 0, 0, 0, 0, 0, 0xC0, 0x20}
	cellXF := []byte{0, 0, 0, 0, 0x01, 0x00, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xC0, 0x20}
	for i := 0; i < 15; i++ {
		w.rec(0x00E0, styleXF)
	}
	w.rec(0x00E0, cellXF)                         // XF 15: cells
	w.rec(0x0293, []byte{0x00, 0x80, 0x00, 0xFF}) // STYLE: Normal
	boundAt := w.buf.Len()
	w.rec(0x0085, append(append(u32b(0), 0, 0), shortStr(sheet)...)) // BOUNDSHEET, offset patched below
	if len(sst) > 0 {
		writeSST(w, sst, total)
	}
	w.rec(0x000A, nil) // EOF

	sheetPos := w.buf.Len()
	bof(0x0010)
	ncols := len(cols)
	w.rec(0x0200, bytes.Join([][]byte{u32b(0), u32b(len(grid)), u16b(0), u16b(ncols), u16b(0)}, nil))               // DIMENSIONS
	w.rec(0x023E, bytes.Join([][]byte{u16b(0x06B6), u16b(0), u16b(0), u32b(0x40), u16b(0), u16b(0), u32b(0)}, nil)) // WINDOW2
	for r, row := range grid {
		for c, v := range row {
			if v == nil {
				continue
			}
			head := bytes.Join([][]byte{u16b(r), u16b(c), u16b(15)}, nil)
			switch v.kind {
			case 's':
				w.rec(0x00FD, append(head, u32b(v.s)...))
			case 'n':
				var f [8]byte
				binary.LittleEndian.PutUint64(f[:], math.Float64bits(v.n))
				w.rec(0x0203, append(head, f[:]...))
			case 'b':
				b := byte(0)
				if v.b {
					b = 1
				}
				w.rec(0x0205, append(head, b, 0))
			}
		}
	}
	w.rec(0x000A, nil)
	out := w.buf.Bytes()
	binary.LittleEndian.PutUint32(out[boundAt+4:], uint32(sheetPos))
	return writeCFB("Workbook", out), nil
}

// writeSST writes the shared string table, continuing it in CONTINUE
// records (a string's characters may split, restarting with a flag byte),
// and the EXTSST index Excel uses to find strings.
func writeSST(w *biffWriter, sst []string, total int) {
	type bucket struct{ pos, inRec int }
	var buckets []bucket
	var cur []byte
	curTyp := uint16(0x00FC)
	curStart := w.buf.Len() // stream offset of the current record header
	flush := func() {
		w.rec(curTyp, cur)
		curTyp, cur, curStart = 0x003C, nil, w.buf.Len()
	}
	cur = append(cur, u32b(total)...)
	cur = append(cur, u32b(len(sst))...)
	// At most 128 buckets keep EXTSST within one record.
	dsst := max(8, len(sst)/128+1)
	for i, s := range sst {
		u := utf16.Encode([]rune(s))
		if biffMaxData-len(cur) < 5 { // header + at least one character
			flush()
		}
		if i%dsst == 0 {
			buckets = append(buckets, bucket{curStart + 4 + len(cur), 4 + len(cur)})
		}
		cur = append(cur, byte(len(u)), byte(len(u)>>8), 1)
		for k := 0; k < len(u); {
			if biffMaxData-len(cur) < 2 {
				flush()
				cur = append(cur, 1) // continued characters: UTF-16 flag
			}
			n := min((biffMaxData-len(cur))/2, len(u)-k)
			for _, c := range u[k : k+n] {
				cur = append(cur, byte(c), byte(c>>8))
			}
			k += n
		}
	}
	flush()
	ext := u16b(dsst)
	for _, b := range buckets {
		ext = append(ext, u32b(b.pos)...)
		ext = append(ext, u16b(b.inRec)...)
		ext = append(ext, u16b(0)...)
	}
	w.rec(0x00FF, ext)
}
