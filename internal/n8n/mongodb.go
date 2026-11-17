package n8n

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/dgmos/flow/internal/node"
)

var mongoPools = newPoolCache(32)

type mongoConn struct {
	client *mongo.Client
	db     string
}

func (m *mongoConn) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.client.Disconnect(ctx)
}

type dialerFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func (f dialerFunc) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return f(ctx, network, addr)
}

func (ec *execCtx) mongo() (*mongoConn, error) {
	c, err := ec.credential("mongoDb")
	if err != nil {
		return nil, err
	}
	return ec.mongoWith(c)
}

// mongoWith opens (or reuses) a client for a mongoDb credential.
func (ec *execCtx) mongoWith(c map[string]interface{}) (*mongoConn, error) {
	uri := credString(c, "connectionString")
	db := credString(c, "database")
	if credString(c, "configurationType") != "connectionString" || uri == "" {
		port := int(toFloatDefault(c["port"], 27017))
		auth := ""
		if u := credString(c, "user"); u != "" {
			auth = url.QueryEscape(u) + ":" + url.QueryEscape(credString(c, "password")) + "@"
		}
		uri = fmt.Sprintf("mongodb://%s%s/%s", auth, net.JoinHostPort(credString(c, "host"), strconv.Itoa(port)), url.PathEscape(db))
		if toBool(c["tls"]) {
			uri += "?tls=true"
		}
	}
	if db == "" {
		if u, err := url.Parse(uri); err == nil {
			db = strings.TrimPrefix(u.Path, "/")
		}
	}
	key := secretKey(uri + "\x00" + mustJSON(ec.opts.Outbound))
	pool, err := mongoPools.get(key, func() (io.Closer, error) {
		opts := options.Client().ApplyURI(uri).SetDialer(dialerFunc(node.NewOutboundDialer(ec.opts.Outbound))).
			SetMaxPoolSize(5).SetServerSelectionTimeout(15 * time.Second)
		client, err := mongo.Connect(opts)
		if err != nil {
			return nil, permanentError{"mongodb: " + redactSecrets(err.Error())}
		}
		return &mongoConn{client: client, db: db}, nil
	})
	if err != nil {
		return nil, err
	}
	return pool.(*mongoConn), nil
}

func mongoDbNode(ec *execCtx) ([][]Item, error) {
	conn, err := ec.mongo()
	if err != nil {
		return nil, err
	}
	if conn.db == "" {
		return nil, permanentError{"MongoDB: no database in the credential"}
	}
	db := conn.client.Database(conn.db)
	items := ec.items()
	op, _ := ec.str(0, "operation", "find")
	collName, _ := ec.str(0, "collection", "")
	coll := db.Collection(collName)
	ctx := ec.ctx
	switch op {
	case "find", "aggregate", "delete":
		return eachItem(ec, func(i int, it Item) ([]Item, error) {
			q, err := ec.str(i, "query", map[string]string{"aggregate": "[]"}[op])
			if err != nil {
				return nil, err
			}
			if q == "" {
				q = "{}"
			}
			if op == "aggregate" {
				var pipeline []interface{}
				if err := bson.UnmarshalExtJSON([]byte(q), false, &pipeline); err != nil {
					return nil, permanentError{"MongoDB: pipeline must be a JSON array: " + err.Error()}
				}
				cur, err := coll.Aggregate(ctx, pipeline)
				if err != nil {
					return nil, permanentError{"mongodb: " + err.Error()}
				}
				return mongoItems(ctx, cur)
			}
			filter, err := mongoFilter(q)
			if err != nil {
				return nil, err
			}
			if op == "delete" {
				res, err := coll.DeleteMany(ctx, filter)
				if err != nil {
					return nil, permanentError{"mongodb: " + err.Error()}
				}
				return one(map[string]interface{}{"deletedCount": float64(res.DeletedCount)}), nil
			}
			fo := options.Find()
			o := fieldsOf(ec, i, "options")
			if l := toFloatDefault(o["limit"], 0); l > 0 {
				fo.SetLimit(int64(l))
			}
			if s := toFloatDefault(o["skip"], 0); s > 0 {
				fo.SetSkip(int64(s))
			}
			for k, set := range map[string]func(interface{}) *options.FindOptionsBuilder{"sort": fo.SetSort, "projection": fo.SetProjection} {
				if s := toString(o[k]); s != "" {
					var m bson.D
					if err := bson.UnmarshalExtJSON([]byte(s), false, &m); err != nil {
						return nil, permanentError{fmt.Sprintf("MongoDB: %s must be a JSON object", k)}
					}
					set(m)
				}
			}
			cur, err := coll.Find(ctx, filter, fo)
			if err != nil {
				return nil, permanentError{"mongodb: " + err.Error()}
			}
			return mongoItems(ctx, cur)
		})
	case "insert":
		docs := make([]interface{}, len(items))
		for i := range items {
			docs[i] = mongoDoc(ec, i, items[i], "")
		}
		if len(docs) == 0 {
			return [][]Item{nil}, nil
		}
		res, err := coll.InsertMany(ctx, docs)
		if err != nil {
			return nil, permanentError{"mongodb: " + err.Error()}
		}
		out := make([]Item, len(items))
		for i, it := range items {
			j := cloneJSON(it.JSON)
			if i < len(res.InsertedIDs) {
				j["id"] = mongoValue(res.InsertedIDs[i])
			}
			out[i] = Item{JSON: j, PairedItem: paired(i)}
		}
		return [][]Item{out}, nil
	case "update", "findOneAndUpdate", "findOneAndReplace":
		return eachItem(ec, func(i int, it Item) ([]Item, error) {
			key, _ := ec.str(i, "updateKey", "id")
			key = strings.TrimSpace(key)
			doc := mongoDoc(ec, i, it, key)
			kv, ok := it.JSON[key]
			if !ok {
				return nil, permanentError{fmt.Sprintf("MongoDB: item has no update key %q", key)}
			}
			filter := bson.M{key: mongoID(key, kv)}
			upsert, _ := ec.boolean(i, "options.upsert", false)
			var err error
			switch op {
			case "update":
				_, err = coll.UpdateOne(ctx, filter, bson.M{"$set": doc}, options.UpdateOne().SetUpsert(upsert))
			case "findOneAndUpdate":
				err = coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": doc}, options.FindOneAndUpdate().SetUpsert(upsert)).Err()
			default:
				err = coll.FindOneAndReplace(ctx, filter, doc, options.FindOneAndReplace().SetUpsert(upsert)).Err()
			}
			if err != nil && err != mongo.ErrNoDocuments {
				return nil, permanentError{"mongodb: " + err.Error()}
			}
			return []Item{{JSON: it.JSON}}, nil
		})
	}
	return nil, unsupportedOp("MongoDB", "document", op)
}

// mongoDoc builds the document to write from the item's selected fields
// (all fields when none are listed), excluding the update key.
func mongoDoc(ec *execCtx, i int, it Item, exclude string) bson.M {
	fields, _ := ec.str(i, "fields", "")
	dot, _ := ec.boolean(i, "options.useDotNotation", false)
	dates := map[string]bool{}
	if d, _ := ec.str(i, "options.dateFields", ""); d != "" {
		for _, f := range splitList(d) {
			dates[f] = true
		}
	}
	doc := bson.M{}
	add := func(k string, v interface{}) {
		if k == exclude {
			return
		}
		if dates[k] {
			if t, ok := timeOf(v); ok {
				v = t
			}
		}
		doc[k] = v
	}
	if strings.TrimSpace(fields) == "" {
		for k, v := range it.JSON {
			add(k, v)
		}
		return doc
	}
	for _, f := range splitList(fields) {
		v, _ := getPath(it.JSON, f, dot)
		add(f, v)
	}
	return doc
}

func mongoID(key string, v interface{}) interface{} {
	if key == "_id" {
		if id, err := bson.ObjectIDFromHex(toString(v)); err == nil {
			return id
		}
	}
	return v
}

func mongoFilter(q string) (bson.M, error) {
	var m bson.M
	if err := bson.UnmarshalExtJSON([]byte(q), false, &m); err != nil {
		return nil, permanentError{"MongoDB: query must be a JSON object: " + err.Error()}
	}
	if s, ok := m["_id"].(string); ok {
		if id, err := bson.ObjectIDFromHex(s); err == nil {
			m["_id"] = id
		}
	}
	return m, nil
}

func mongoItems(ctx context.Context, cur *mongo.Cursor) ([]Item, error) {
	defer func() { _ = cur.Close(ctx) }()
	var out []Item
	for cur.Next(ctx) {
		var d bson.M
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		j, _ := mongoValue(d).(map[string]interface{})
		out = append(out, Item{JSON: j})
	}
	return out, cur.Err()
}

// mongoValue converts BSON values to JSON-compatible ones (ObjectIDs as hex
// strings, dates as RFC 3339, numbers as float64).
func mongoValue(v interface{}) interface{} {
	switch t := v.(type) {
	case bson.M:
		out := make(map[string]interface{}, len(t))
		for k, x := range t {
			out[k] = mongoValue(x)
		}
		return out
	case bson.D:
		out := make(map[string]interface{}, len(t))
		for _, e := range t {
			out[e.Key] = mongoValue(e.Value)
		}
		return out
	case bson.A:
		out := make([]interface{}, len(t))
		for k, x := range t {
			out[k] = mongoValue(x)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for k, x := range t {
			out[k] = mongoValue(x)
		}
		return out
	case bson.ObjectID:
		return t.Hex()
	case bson.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case bson.Decimal128:
		f, err := strconv.ParseFloat(t.String(), 64)
		if err != nil {
			return t.String()
		}
		return f
	case nil, string, bool, float64:
		return t
	default:
		b, err := bson.MarshalExtJSON(bson.M{"v": t}, false, false)
		if err != nil {
			return fmt.Sprint(t)
		}
		var m map[string]interface{}
		_ = json.Unmarshal(b, &m)
		return m["v"]
	}
}
