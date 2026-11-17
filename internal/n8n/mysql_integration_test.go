//go:build integration

package n8n

import (
	"database/sql"
	"net"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dgmos/flow/internal/node"
	"github.com/dgmos/flow/internal/storage/integrationtest"
)

func TestMySQLTableOperations(t *testing.T) {
	cfg, err := mysql.ParseDSN(integrationtest.MySQLDSN())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	for _, q := range []string{"DROP TABLE IF EXISTS n8n_people", "CREATE TABLE n8n_people (id INT PRIMARY KEY, name VARCHAR(50), city VARCHAR(50))"} {
		if _, err := raw.Exec(q); err != nil {
			t.Skipf("mysql not available: %v", err)
		}
	}
	host, portStr, _ := net.SplitHostPort(cfg.Addr)
	port, _ := strconv.Atoi(portStr)
	creds := NewStaticCredentials()
	creds.Set("mySql", "*", map[string]interface{}{"host": host, "port": port, "user": cfg.User, "password": cfg.Passwd, "database": cfg.DBName})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{host}}})
	my := func(name string, v float64, p map[string]interface{}) map[string]interface{} {
		return credNode(nd(name, "mySql", v, p), "mySql")
	}
	tbl := map[string]interface{}{"__rl": true, "mode": "name", "value": "n8n_people"}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Rows", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":[{"id":1,"name":"Ada","city":"London"},{"id":2,"name":"Bob","city":"Paris"}]}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		my("Insert v1", 1, map[string]interface{}{"table": tbl, "columns": "id, name, city"}),
		my("Update v1", 1, map[string]interface{}{"operation": "update", "table": tbl, "updateKey": "id", "columns": "city"}),
		my("Upsert v2", 2.4, map[string]interface{}{"operation": "upsert", "table": tbl, "columns": map[string]interface{}{"mappingMode": "defineBelow",
			"matchingColumns": []interface{}{"id"}, "value": map[string]interface{}{"id": 3, "name": "Cem", "city": "Izmir"}}}),
		my("Select v2", 2.4, map[string]interface{}{"operation": "select", "table": tbl, "returnAll": true,
			"where": map[string]interface{}{"values": []interface{}{map[string]interface{}{"column": "id", "condition": "gte", "value": 1}}}}),
	}, "Start->Rows", "Rows->Split", "Split->Insert v1", "Insert v1->Update v1", "Update v1->Upsert v2", "Upsert v2->Select v2")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["Insert v1"], 0); len(got) != 2 || got[0]["affectedRows"] != 1.0 {
		t.Fatalf("insert: %v", got)
	}
	got := itemsOf(t, steps["Select v2"], 0)
	// Select runs once per input item (2 items x 3 rows), as in n8n.
	if len(got) != 6 || got[2]["name"] != "Cem" {
		t.Fatalf("select after upsert: %v", got)
	}
}
