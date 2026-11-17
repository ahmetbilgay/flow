package n8n

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"mime"
	"path"
	"strings"

	"github.com/google/uuid"
)

// Decompression limits guard against archive bombs (variables for tests).
var (
	maxDecompressedBytes int64 = 64 << 20
	maxArchiveEntries          = 1000
)

// ---------------------------------------------------------------- Execution Data

// executionDataNode saves custom execution data in n8n; here the values are
// evaluated (so expression errors surface) and the items pass through.
func executionDataNode(ec *execCtx) ([][]Item, error) {
	for i := range ec.items() {
		if _, err := ec.param(i, "dataToSave", nil); err != nil {
			return nil, err
		}
	}
	return passthrough(ec)
}

// ---------------------------------------------------------------- Compression

func compressionNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "decompress")
	var out []Item
	for i, it := range ec.items() {
		props, _ := ec.str(i, "binaryPropertyName", "data")
		var res Item
		var err error
		if op == "compress" {
			res, err = compressItem(ec, i, it, splitList(props))
		} else {
			res, err = decompressItem(ec, i, it, splitList(props))
		}
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		res.PairedItem = paired(i)
		out = append(out, res)
	}
	return [][]Item{out}, nil
}

func compressItem(ec *execCtx, i int, it Item, props []string) (Item, error) {
	format, _ := ec.str(i, "outputFormat", "zip")
	res := Item{JSON: it.JSON, Binary: map[string]interface{}{}}
	if format == "gzip" {
		prefix, _ := ec.str(i, "outputPrefix", "data")
		for n, p := range props {
			data, _, name, err := binaryOf(it, p)
			if err != nil {
				return Item{}, err
			}
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			zw.Name = name
			if _, err := zw.Write(data); err != nil {
				return Item{}, err
			}
			if err := zw.Close(); err != nil {
				return Item{}, err
			}
			res.Binary[fmt.Sprintf("%s%d", prefix, n)] = binaryData(buf.Bytes(), "application/gzip", name+".gz")
		}
		return res, nil
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, p := range props {
		data, _, name, err := binaryOf(it, p)
		if err != nil {
			return Item{}, err
		}
		w, err := zw.Create(name)
		if err != nil {
			return Item{}, err
		}
		if _, err := w.Write(data); err != nil {
			return Item{}, err
		}
	}
	if err := zw.Close(); err != nil {
		return Item{}, err
	}
	name, _ := ec.str(i, "fileName", "data.zip")
	if name == "" {
		name = "data.zip"
	}
	outProp, _ := ec.str(i, "binaryPropertyOutput", "data")
	res.Binary[outProp] = binaryData(buf.Bytes(), "application/zip", name)
	return res, nil
}

func decompressItem(ec *execCtx, i int, it Item, props []string) (Item, error) {
	prefix, _ := ec.str(i, "outputPrefix", "file_")
	res := Item{JSON: it.JSON, Binary: map[string]interface{}{}}
	total, n := int64(0), 0
	add := func(name string, r io.Reader) error {
		if n >= maxArchiveEntries {
			return permanentError{fmt.Sprintf("archive has more than %d files", maxArchiveEntries)}
		}
		data, err := io.ReadAll(io.LimitReader(r, maxDecompressedBytes-total+1))
		if err != nil {
			return err
		}
		if total += int64(len(data)); total > maxDecompressedBytes {
			return permanentError{fmt.Sprintf("decompressed data exceeds %d MiB", maxDecompressedBytes>>20)}
		}
		mt := mime.TypeByExtension(path.Ext(name))
		if mt == "" {
			mt = sniffMime(data)
		}
		res.Binary[fmt.Sprintf("%s%d", prefix, n)] = binaryData(data, mt, path.Base(name))
		n++
		return nil
	}
	for _, p := range props {
		data, mt, name, err := binaryOf(it, p)
		if err != nil {
			return Item{}, err
		}
		switch {
		case mt == "application/zip" || strings.HasSuffix(strings.ToLower(name), ".zip") || bytes.HasPrefix(data, []byte("PK\x03\x04")):
			zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				return Item{}, permanentError{"invalid zip archive: " + err.Error()}
			}
			for _, f := range zr.File {
				if f.FileInfo().IsDir() || strings.HasPrefix(path.Base(f.Name), "._") || strings.HasPrefix(f.Name, "__MACOSX/") {
					continue
				}
				rc, err := f.Open()
				if err != nil {
					return Item{}, err
				}
				err = add(f.Name, rc)
				rc.Close()
				if err != nil {
					return Item{}, err
				}
			}
		case mt == "application/gzip" || mt == "application/x-gzip" || bytes.HasPrefix(data, []byte{0x1f, 0x8b}):
			zr, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return Item{}, permanentError{"invalid gzip data: " + err.Error()}
			}
			inner := zr.Name
			if inner == "" {
				inner = strings.TrimSuffix(name, ".gz")
			}
			err = add(inner, zr)
			zr.Close()
			if err != nil {
				return Item{}, err
			}
		default:
			return Item{}, permanentError{fmt.Sprintf("binary property %q is not a zip or gzip file", p)}
		}
	}
	return res, nil
}

// ---------------------------------------------------------------- Read PDF

func readPDFNode(ec *execCtx) ([][]Item, error) {
	var out []Item
	for i, it := range ec.items() {
		prop, _ := ec.str(i, "binaryPropertyName", "data")
		if enc, _ := ec.boolean(i, "encrypted", false); enc {
			return nil, permanentError{"Read PDF: encrypted PDFs are not supported"}
		}
		data, _, _, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		pages, total, err := pdfText(data, 0)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		out = append(out, Item{JSON: map[string]interface{}{
			"numpages": float64(total), "numrender": float64(len(pages)), "info": map[string]interface{}{},
			"metadata": nil, "text": strings.Join(pages, "\n\n"), "version": "1.10.100",
		}, Binary: it.Binary, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Crypto

func cryptoNode(ec *execCtx) ([][]Item, error) {
	action, _ := ec.str(0, "action", "hash")
	var out []Item
	for i, it := range ec.items() {
		dest, _ := ec.str(i, "dataPropertyName", "data")
		var v string
		switch action {
		case "hash", "hmac":
			typ, _ := ec.str(i, "type", "MD5")
			value, _ := ec.str(i, "value", "")
			enc, _ := ec.str(i, "encoding", "hex")
			var newHash func() hash.Hash
			switch strings.ToUpper(strings.ReplaceAll(typ, "-", "")) {
			case "MD5":
				newHash = md5.New
			case "SHA1":
				newHash = sha1.New
			case "SHA256":
				newHash = sha256.New
			case "SHA384":
				newHash = sha512.New384
			case "SHA512":
				newHash = sha512.New
			default:
				return nil, permanentError{fmt.Sprintf("Crypto: hash type %q is not supported", typ)}
			}
			h := newHash()
			if action == "hmac" {
				secret, _ := ec.str(i, "secret", "")
				h = hmac.New(newHash, []byte(secret))
			}
			h.Write([]byte(value))
			sum := h.Sum(nil)
			if enc == "base64" {
				v = base64.StdEncoding.EncodeToString(sum)
			} else {
				v = hex.EncodeToString(sum)
			}
		case "generate":
			enc, _ := ec.str(i, "encodingType", "uuid")
			if enc == "uuid" {
				v = uuid.NewString()
				break
			}
			n, _ := ec.num(i, "stringLength", 32)
			if n < 1 || n > 4096 {
				return nil, permanentError{"Crypto: string length must be between 1 and 4096"}
			}
			buf := make([]byte, int(n))
			if _, err := rand.Read(buf); err != nil {
				return nil, err
			}
			switch enc {
			case "base64":
				v = base64.StdEncoding.EncodeToString(buf)[:int(n)]
			case "hex":
				v = hex.EncodeToString(buf)[:int(n)]
			default: // ascii
				const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
				for k := range buf {
					buf[k] = chars[int(buf[k])%len(chars)]
				}
				v = string(buf)
			}
		default:
			return nil, permanentError{fmt.Sprintf("Crypto: action %q is not supported", action)}
		}
		j := cloneJSON(it.JSON)
		if j == nil {
			j = map[string]interface{}{}
		}
		setPath(j, dest, v, true)
		out = append(out, Item{JSON: j, Binary: it.Binary, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}
