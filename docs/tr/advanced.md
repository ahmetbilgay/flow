# İleri Düzey Özellikler

Sistemin ileri düzey özellikleri: Esnek Yetkilendirme (Authorization), Cron Ticker'ları, Webhook'lar ve Sistem İzleme.

---

## 🔒 Esnek Yetkilendirme (AuthProvider)

Dgmos Flow, iş akışı tanımları ve çalıştırmaları üzerinde yetkilendirmeyi `AuthProvider` arayüzü ile yönetir.

```go
type AuthProvider interface {
    CheckAccess(ctx context.Context, resource string, action string) error
}
```

### 1. Yetkilendirme Olmadan Kullanım (Geliştirme)
Varsayılan olarak, motor oluşturulduğunda herhangi bir yetkilendirici atanmaz ve tüm işlemlere izin verilir:
```go
engine := flow.NewEngine(storage, registry)
```

### 2. Özel RBAC & Kiracı İzolasyonu (Tenant Isolation)
Kullanıcı ID'leri, Kiracı ID'leri, Roller veya JWT jetonları gibi kimlik doğrulama bilgilerini `context.Context` üzerinden taşıyabilir ve `CheckAccess` içinde denetleyebilirsiniz:

```go
type CustomAuth struct{}

func (a *CustomAuth) CheckAccess(ctx context.Context, resource string, action string) error {
	// 1. Context üzerinden kiracı bilgisini alın
	customerID, _ := ctx.Value("customer_id").(string)
	if customerID == "" {
		return errors.New("unauthorized: missing customer_id")
	}

	// 2. Format: "workflow:namespace:workflow-id"
	// Kaynak bilgilerini ayrıştırıp sahiplik kontrolü yapın
	// ...
	
	return nil
}

// Auth Provider Tanımlama
engine.SetAuthProvider(&CustomAuth{})
```

---

## ⏱️ Zamanlanmış Görevler (Cron Manager)

Kalıcı, çoklu-instance güvenli cron; IANA timezone, catch-up, zaman penceresi ve REST API.
`Pause` = soft disable; `DELETE` = kalıcı silme. `ends_at` dolunca auth’suz auto-pause.

```go
ctx := context.Background()
_ = engine.CronManager.Start(ctx)
defer engine.CronManager.Stop()

id, err := engine.CronManager.AddScheduleWithOpts(ctx, "tenant-a", "db-backup-wf", "0 0 9 * * *", `{"type":"daily"}`, flow.ScheduleOpts{
	Name:     "sabah-yedek",
	Timezone: "Europe/Istanbul",
	CatchUp:  true,
})
expr := "0 30 9 * * *"
_ = engine.CronManager.UpdateSchedule(ctx, "tenant-a", id, flow.ScheduleUpdateOpts{CronExpression: &expr})

api := flow.NewAPIManager(flow.NewWebhookManager(engine)).WithCron(engine.CronManager)
http.Handle("/", api)
```

REST:
- `GET|POST /api/{namespace}/schedules` (`?active_only=true&limit=&offset=`)
- `GET|PUT|DELETE /api/{namespace}/schedules/{id}` (PUT kısmi update)
- `POST /api/{namespace}/schedules/{id}/pause|resume`
- `POST /api/{namespace}/schedules/preview`

---

## 📡 Webhook Yönetimi (Webhook Manager)

İş akışlarını dış sistemlerden tetiklemek için standart HTTP POST isteklerini kullanabilirsiniz. `WebhookManager` gelen istekleri yakalar ve akışları tetikler.

```go
webhookMgr := flow.NewWebhookManager(engine)

// Endpoint haritalama: /webhooks/{namespace}/{workflow_id}
http.Handle("/webhooks/", webhookMgr)
log.Fatal(http.ListenAndServe(":8080", nil))
```
Terminal üzerinden tetikleme örneği:
```bash
curl -X POST http://localhost:8080/webhooks/default/my-workflow -d '{"data": "value"}'
```

---

## 📊 Telemetri & İzleme (Monitoring)

### 1. Prometheus Metrikleri
Kütüphane içinde tanımlanan tüm metrikler önceden kaydedilmiş durumdadır.

```go
import (
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

http.Handle("/metrics", promhttp.Handler())
```

*   **İş Akışı Metrikleri**:
    *   `flow_workflows_started_total` (Başlatılan toplam akış)
    *   `flow_workflows_completed_total` (Başarıyla tamamlanan toplam akış)
    *   `flow_workflows_failed_total` (Başarısız olan toplam akış)
*   **Adım Metrikleri**:
    *   `flow_steps_processed_total` (İşlenen adımlar, Etiketler: `namespace`, `node_type`, `status`)
    *   `flow_step_duration_seconds` (Adım işleme süreleri, Etiketler: `namespace`, `node_type`)
*   **İşçi (Worker) Metrikleri**:
    *   `flow_active_workers` (Aktif işçi sayısı, Etiketler: `worker_id`)

### 2. Yapılandırılmış Loglama (Structured Logging)
Özel bir `slog.Logger` nesnesini motora enjekte edebilirsiniz:
```go
import "log/slog"

logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
engine.WithLogger(logger)
```

---

## Interactive Advance ve Human Task (DGMOS-ready)

İnsan/API güdümlü adımlar için worker claim edilmez; `humantask` / `execution_mode: interactive` adımlar doğrudan `WAITING` oluşur.

```go
_, next, _ := engine.CompleteTask(ctx, "default", execID, "approve", "user@acme.com", "approve", `{"comment":"ok"}`)
_, next, _ = engine.Advance(ctx, "default", execID, "next", "submit", `{}`)
```

`SetHostHooks` ile WS/notification bind edin. Değişkenler: `GetVar` / `SetVar` / `MergeVars`. SQL prefix: `flow.SQLStorageOptions{TablePrefix: "flow_"}`. Auth kaynakları: `workflow:{ns}:{id}`, `execution:{ns}:{id}`, `task:{ns}:{stepId}`.

Örnek: `examples/interactive_approval/`.
