package n8n

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------------- PGVector

type pgVectorStore struct {
	db                            *sql.DB
	table, id, vec, content, meta string
	op                            string // distance operator
	scoreOf                       func(d float64) float64
	ensured                       bool
}

func (ec *execCtx) openPGVector(c, p map[string]interface{}) (vectorStore, error) {
	db, err := ec.postgresDBWith(c)
	if err != nil {
		return nil, err
	}
	cols := asMap(asMap(asMap(p["options"])["columnNames"])["values"])
	name := func(k, def string) string { return firstNonEmpty(toString(cols[k]), def) }
	s := &pgVectorStore{db: db, table: firstNonEmpty(rlValue(p["tableName"]), "n8n_vectors"),
		id: name("idColumnName", "id"), vec: name("vectorColumnName", "embedding"), content: name("contentColumnName", "text"), meta: name("metadataColumnName", "metadata"),
		op: "<=>", scoreOf: func(d float64) float64 { return 1 - d }}
	switch toString(asMap(p["options"])["distanceStrategy"]) {
	case "innerProduct":
		s.op, s.scoreOf = "<#>", func(d float64) float64 { return -d }
	case "euclidean":
		s.op, s.scoreOf = "<->", func(d float64) float64 { return 1 / (1 + d) }
	}
	return s, nil
}

func (s *pgVectorStore) q(name string) string { return pq.QuoteIdentifier(name) }

func (s *pgVectorStore) ensure(ctx context.Context) error {
	if s.ensured {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		return permanentError{"pgvector: " + err.Error()}
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s uuid PRIMARY KEY DEFAULT gen_random_uuid(), %s text, %s jsonb, %s vector)",
		s.q(s.table), s.q(s.id), s.q(s.content), s.q(s.meta), s.q(s.vec)))
	if err != nil {
		return permanentError{"pgvector: " + err.Error()}
	}
	s.ensured = true
	return nil
}

func vectorLiteral(v []float64) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(f, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func (s *pgVectorStore) add(ctx context.Context, docs []document, vecs [][]float64) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, $3::vector)", s.q(s.table), s.q(s.content), s.q(s.meta), s.q(s.vec))
	for i, d := range docs {
		if _, err := s.db.ExecContext(ctx, stmt, d.PageContent, mustJSON(d.Metadata), vectorLiteral(vecs[i])); err != nil {
			return permanentError{"pgvector: " + err.Error()}
		}
	}
	return nil
}

func (s *pgVectorStore) search(ctx context.Context, vec []float64, k int) ([]scoredDoc, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT %s, %s::text, %s %s $1::vector AS d FROM %s ORDER BY d LIMIT %d",
		s.q(s.content), s.q(s.meta), s.q(s.vec), s.op, s.q(s.table), k), vectorLiteral(vec))
	if err != nil {
		return nil, permanentError{"pgvector: " + err.Error()}
	}
	defer rows.Close()
	var out []scoredDoc
	for rows.Next() {
		var content, meta sql.NullString
		var d float64
		if err := rows.Scan(&content, &meta, &d); err != nil {
			return nil, err
		}
		var m map[string]interface{}
		_ = jsonUnmarshal([]byte(meta.String), &m)
		out = append(out, scoredDoc{document{PageContent: content.String, Metadata: m}, s.scoreOf(d)})
	}
	return out, rows.Err()
}

func (s *pgVectorStore) clear(ctx context.Context) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM "+s.q(s.table))
	return err
}

func (s *pgVectorStore) update(ctx context.Context, id string, d document, vec []float64) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s = $1, %s = $2, %s = $3::vector WHERE %s::text = $4", s.q(s.table), s.q(s.content), s.q(s.meta), s.q(s.vec), s.q(s.id)),
		d.PageContent, mustJSON(d.Metadata), vectorLiteral(vec), id)
	return err
}

// ---------------------------------------------------------------- MongoDB Atlas Vector Search

type mongoAtlasStore struct {
	conn                  *mongoConn
	collection, index     string
	embeddingKey, textKey string
}

func (ec *execCtx) openMongoAtlas(s subNode, p map[string]interface{}) (vectorStore, error) {
	c, err := ec.subCredential(s, "mongoDb")
	if err != nil {
		return nil, err
	}
	conn, err := ec.mongoWith(c)
	if err != nil {
		return nil, err
	}
	opts := asMap(p["options"])
	return &mongoAtlasStore{conn: conn, collection: rlValue(p["mongoCollection"]), index: rlValue(p["vectorIndexName"]),
		embeddingKey: firstNonEmpty(toString(p["embedding"]), firstNonEmpty(toString(opts["embedding"]), "embedding")),
		textKey:      firstNonEmpty(toString(p["metadata_field"]), firstNonEmpty(toString(opts["metadata_field"]), "text"))}, nil
}

func (m *mongoAtlasStore) add(ctx context.Context, docs []document, vecs [][]float64) error {
	c := m.conn.client.Database(m.conn.db).Collection(m.collection)
	rows := make([]interface{}, len(docs))
	for i, d := range docs {
		doc := bson.M{}
		for k, v := range d.Metadata {
			doc[k] = v
		}
		doc[m.textKey], doc[m.embeddingKey] = d.PageContent, vecs[i]
		rows[i] = doc
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := c.InsertMany(ctx, rows)
	return err
}

func (m *mongoAtlasStore) search(ctx context.Context, vec []float64, k int) ([]scoredDoc, error) {
	c := m.conn.client.Database(m.conn.db).Collection(m.collection)
	pipeline := []interface{}{
		bson.M{"$vectorSearch": bson.M{"index": m.index, "path": m.embeddingKey, "queryVector": vec, "numCandidates": k * 10, "limit": k}},
		bson.M{"$set": bson.M{"score": bson.M{"$meta": "vectorSearchScore"}}},
		bson.M{"$project": bson.M{m.embeddingKey: 0}},
	}
	cur, err := c.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, permanentError{"mongodb atlas vector search: " + err.Error()}
	}
	items, err := mongoItems(ctx, cur)
	if err != nil {
		return nil, err
	}
	var out []scoredDoc
	for _, it := range items {
		content := toString(it.JSON[m.textKey])
		score := toFloatDefault(it.JSON["score"], 0)
		meta := map[string]interface{}{}
		for k, v := range it.JSON {
			if k != m.textKey && k != "score" && k != "_id" {
				meta[k] = v
			}
		}
		out = append(out, scoredDoc{document{PageContent: content, Metadata: meta}, score})
	}
	return out, nil
}

func (m *mongoAtlasStore) clear(ctx context.Context) error {
	_, err := m.conn.client.Database(m.conn.db).Collection(m.collection).DeleteMany(ctx, bson.M{})
	return err
}

// ---------------------------------------------------------------- Cohere reranker

// rerank reorders search results with a connected ai_reranker sub-node.
func (ec *execCtx) rerank(cfg map[string]interface{}, query string, docs []scoredDoc, i int) ([]scoredDoc, error) {
	subs := aiSubNodes(cfg, "ai_reranker")
	if len(subs) == 0 || len(docs) == 0 {
		return docs, nil
	}
	s := subs[0]
	if short(s.typ) != "rerankerCohere" {
		return nil, permanentError{fmt.Sprintf("reranker %s is not supported", s.typ)}
	}
	p, err := ec.resolveSub(s, i)
	if err != nil {
		return nil, err
	}
	c, err := ec.subCredential(s, "cohereApi")
	if err != nil {
		return nil, err
	}
	texts := make([]string, len(docs))
	for k, d := range docs {
		texts[k] = d.PageContent
	}
	topN := len(docs)
	if n := int(toFloatDefault(asMap(p["options"])["topN"], 0)); n > 0 && n < topN {
		topN = n
	}
	body := map[string]interface{}{"model": firstNonEmpty(toString(p["modelName"]), "rerank-v3.5"), "query": query, "documents": texts, "top_n": topN}
	res, _, err := ec.apiCall("POST", credBaseURL(c, ec.apiBase("cohere", "https://api.cohere.com"))+"/v2/rerank", map[string]string{"Authorization": "Bearer " + credString(c, "apiKey")}, body, "")
	if err != nil {
		return nil, err
	}
	var out []scoredDoc
	for _, r := range asList(res["results"]) {
		rm := asMap(r)
		idx := int(toFloatDefault(rm["index"], -1))
		if idx >= 0 && idx < len(docs) {
			d := docs[idx]
			d.Score = toFloatDefault(rm["relevance_score"], d.Score)
			out = append(out, d)
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].Score > out[b].Score })
	return out, nil
}
