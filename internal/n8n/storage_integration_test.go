//go:build integration

package n8n

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/dgmos/flow/internal/node"
	"github.com/dgmos/flow/internal/storage/integrationtest"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// S3 against MinIO: real SigV4 verification for bucket create, upload,
// list, download and delete.
func TestS3AgainstMinIO(t *testing.T) {
	endpoint := envOr("FLOW_S3_ENDPOINT", "http://127.0.0.1:9000")
	creds := NewStaticCredentials()
	creds.Set("s3", "*", map[string]interface{}{"endpoint": endpoint, "region": "us-east-1", "accessKeyId": envOr("FLOW_S3_KEY", "minioadmin"), "secretAccessKey": envOr("FLOW_S3_SECRET", "minioadmin"), "forcePathStyle": true})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1", "localhost", "minio"}}})
	bucket := "flow-n8n-" + strings.ToLower(integrationtest.UniqueID())
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	s3n := func(name string, p map[string]interface{}) map[string]interface{} {
		return credNode(nd(name, "s3", 1, p), "s3")
	}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		s3n("Bucket", map[string]interface{}{"resource": "bucket", "operation": "create", "name": bucket}),
		s3n("Upload", map[string]interface{}{"operation": "upload", "bucketName": bucket, "fileName": "dir/hello world.txt", "binaryData": false, "fileContent": "héllo",
			"tagsUi": map[string]interface{}{"tagsValues": []interface{}{map[string]interface{}{"key": "env", "value": "test"}}}}),
		s3n("List", map[string]interface{}{"operation": "getAll", "bucketName": bucket, "returnAll": true}),
		s3n("Download", map[string]interface{}{"operation": "download", "bucketName": bucket, "fileKey": "dir/hello world.txt"}),
		s3n("Delete", map[string]interface{}{"operation": "delete", "bucketName": bucket, "fileKey": "dir/hello world.txt"}),
	}, "Start->Bucket", "Bucket->Upload", "Upload->List", "List->Download", "Download->Delete")
	_, steps := h.run(t, data, "")
	if steps["Bucket"].Status != "COMPLETED" {
		integrationtest.SkipIfUnavailable(t, fmt.Errorf("s3 not available: %v", steps["Bucket"].Error))
	}
	if got := itemsOf(t, steps["List"], 0); len(got) != 1 || got[0]["Key"] != "dir/hello world.txt" {
		t.Fatalf("list: %v (upload error: %s)", got, steps["Upload"].Error)
	}
	out := decodeOutput(steps["Download"].Output)[0]
	if b, _, name, err := binaryOf(out[0], "data"); err != nil || string(b) != "héllo" || name != "hello world.txt" {
		t.Fatalf("download: %q %q %v", b, name, err)
	}
	if steps["Delete"].Status != "COMPLETED" {
		t.Fatalf("delete: %s", steps["Delete"].Error)
	}
}

func TestMongoDBOperations(t *testing.T) {
	creds := NewStaticCredentials()
	creds.Set("mongoDb", "*", map[string]interface{}{"configurationType": "connectionString", "connectionString": envOr("FLOW_MONGO_URI", "mongodb://127.0.0.1:27017/flow_n8n_test"), "database": "flow_n8n_test"})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1", "localhost", "mongo"}}})
	mg := func(name string, v float64, p map[string]interface{}) map[string]interface{} {
		return credNode(nd(name, "mongoDb", v, p), "mongoDb")
	}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		mg("Clean", 1.2, map[string]interface{}{"operation": "delete", "collection": "people", "query": "{}"}),
		nd("Rows", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":[{"id":1,"name":"Ada","city":"London"},{"id":2,"name":"Bob","city":"Paris"}]}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		mg("Insert", 1.2, map[string]interface{}{"operation": "insert", "collection": "people", "fields": "id,name,city", "options": map[string]interface{}{}}),
		mg("Update", 1.2, map[string]interface{}{"operation": "update", "collection": "people", "updateKey": "id", "fields": "city", "options": map[string]interface{}{}}),
		mg("Find", 1.2, map[string]interface{}{"collection": "people", "query": `{"name":"Ada"}`, "options": map[string]interface{}{}}),
	}, "Start->Clean", "Clean->Rows", "Rows->Split", "Split->Insert", "Insert->Update", "Update->Find")
	_, steps := h.run(t, data, "")
	if steps["Clean"].Status != "COMPLETED" {
		integrationtest.SkipIfUnavailable(t, fmt.Errorf("mongodb not available: %v", steps["Clean"].Error))
	}
	ins := itemsOf(t, steps["Insert"], 0)
	if len(ins) != 2 || len(toString(ins[0]["id"])) != 24 && ins[0]["id"] == nil {
		t.Fatalf("insert: %v", ins)
	}
	got := itemsOf(t, steps["Find"], 0)
	// Find runs once per input item (2), each returning Ada.
	if len(got) != 2 || got[0]["city"] != "London" || len(toString(got[0]["_id"])) != 24 {
		t.Fatalf("find: %v", got)
	}
}

// PGVector against PostgreSQL with the pgvector extension (FLOW_PGVECTOR_DSN).
func TestPGVectorStore(t *testing.T) {
	dsn := os.Getenv("FLOW_PGVECTOR_DSN")
	if dsn == "" {
		t.Skip("FLOW_PGVECTOR_DSN not set (PostgreSQL with pgvector)")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	srv, _ := aiFake(t)
	creds := NewStaticCredentials()
	creds.Set("postgres", "*", map[string]interface{}{"host": u.Hostname(), "port": u.Port(), "user": u.User.Username(), "password": pass, "database": strings.TrimPrefix(u.Path, "/")})
	creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": "k", "url": srv.URL})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1", u.Hostname()}}})
	table := "n8n_vec_" + strings.ReplaceAll(integrationtest.UniqueID()[:8], "-", "")
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Docs", "code", 2, map[string]interface{}{"jsCode": "return [{json:{text:'the cat sat on the mat'}},{json:{text:'pasta recipe'}}];"}),
		withCred(lc("Insert", "vectorStorePGVector", 1, map[string]interface{}{"mode": "insert", "tableName": table}), "postgres"),
		lc("Loader", "documentDefaultDataLoader", 1, map[string]interface{}{"jsonMode": "expressionData", "jsonData": "={{ $json.text }}"}),
		withCred(lc("Emb1", "embeddingsOpenAi", 1.2, nil), "openAiApi"),
		withCred(lc("Find", "vectorStorePGVector", 1, map[string]interface{}{"mode": "load", "tableName": table, "prompt": "cat on a mat", "topK": 1}), "postgres"),
		withCred(lc("Emb2", "embeddingsOpenAi", 1.2, nil), "openAiApi"),
	}, "Start->Docs", "Docs->Insert", "Loader~ai_document->Insert", "Emb1~ai_embedding->Insert", "Insert->Find", "Emb2~ai_embedding->Find")
	_, steps := h.run(t, data, "")
	if steps["Insert"].Status != "COMPLETED" {
		integrationtest.SkipIfUnavailable(t, fmt.Errorf("pgvector: %s", steps["Insert"].Error))
	}
	got := itemsOf(t, steps["Find"], 0)
	if len(got) == 0 || asMap(got[0]["document"])["pageContent"] != "the cat sat on the mat" {
		t.Fatalf("pgvector search: %v", got)
	}
}

// MongoDB Atlas vector store: insert works on any MongoDB; $vectorSearch
// needs Atlas, so only the insert path is checked here.
func TestMongoAtlasInsert(t *testing.T) {
	srv, _ := aiFake(t)
	creds := NewStaticCredentials()
	creds.Set("mongoDb", "*", map[string]interface{}{"configurationType": "connectionString", "connectionString": envOr("FLOW_MONGO_URI", "mongodb://127.0.0.1:27017/flow_n8n_test"), "database": "flow_n8n_test"})
	creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": "k", "url": srv.URL})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1", "localhost", "mongo"}}})
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Docs", "code", 2, map[string]interface{}{"jsCode": "return [{json:{text:'hello atlas'}}];"}),
		withCred(lc("Insert", "vectorStoreMongoDBAtlas", 1, map[string]interface{}{"mode": "insert", "mongoCollection": map[string]interface{}{"__rl": true, "mode": "id", "value": "vecs"}, "vectorIndexName": "idx"}), "mongoDb"),
		lc("Loader", "documentDefaultDataLoader", 1, map[string]interface{}{"jsonMode": "expressionData", "jsonData": "={{ $json.text }}"}),
		withCred(lc("Emb", "embeddingsOpenAi", 1.2, nil), "openAiApi"),
		credNode(nd("Check", "mongoDb", 1.2, map[string]interface{}{"collection": "vecs", "query": `{"text":"hello atlas"}`, "options": map[string]interface{}{}}), "mongoDb"),
	}, "Start->Docs", "Docs->Insert", "Loader~ai_document->Insert", "Emb~ai_embedding->Insert", "Insert->Check")
	_, steps := h.run(t, data, "")
	if steps["Insert"].Status != "COMPLETED" {
		integrationtest.SkipIfUnavailable(t, fmt.Errorf("mongodb: %s", steps["Insert"].Error))
	}
	got := itemsOf(t, steps["Check"], 0)
	if len(got) == 0 || len(asList(got[0]["embedding"])) == 0 {
		t.Fatalf("atlas insert: %v", got)
	}
}
