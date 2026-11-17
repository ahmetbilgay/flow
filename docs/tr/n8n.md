# n8n Workflow'ları

Flow, [n8n](https://n8n.io) workflow export'larını içe aktarıp çalıştırabilir. Her n8n node'u kalıcı bir engine adımına dönüşür. Böylece içe aktarılan workflow'lar n8n'in veri modelini (item'lar, paired item'lar, ifadeler) korurken flow'un retry, lease, eşzamanlılık ve kalıcılık özelliklerinden yararlanır.

En çok görüntülenen 299 public n8n şablonu üzerinde yapılan ölçümde **%76'sı tam çalışabilir** olarak içe aktarılıyor. Bu, tüm node ve özelliklerin destekleniyor olduğu anlamına gelir; credential'ların yine de sağlanması gerekir. Diğerleri için importer eksik olanı tam olarak raporlar.

## Hızlı başlangıç

```go
eng := flow.NewEngine(storage, flow.NewRegistry())

creds := flow.NewN8NStaticCredentials() // ya da kendi vault tabanlı provider'ınız
creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": os.Getenv("OPENAI_API_KEY")})

eng.EnableN8N(flow.N8NOptions{
    Credentials:   creds,
    ResumeBaseURL: "https://app.example.com/n8n", // N8NHandler'ın mount edildiği adres
    ResumeSecret:  os.Getenv("N8N_RESUME_SECRET"),
})

data, _ := os.ReadFile("workflow.json") // n8n export'u veya şablon JSON'u
res, err := eng.InstallN8NWorkflow(ctx, "default", data, flow.N8NInstallOptions{})
if errors.Is(err, flow.ErrN8NNotRunnable) {
    for _, b := range res.Report.Blocking {
        log.Printf("desteklenmiyor: %s (%s): %s", b.Node, b.Type, b.Message)
    }
}

http.Handle("/n8n/", http.StripPrefix("/n8n", eng.N8NHandler(flow.N8NHandlerOptions{Namespace: "default"})))
go eng.CronManager.Start(ctx) // zamanlanmış ve polling trigger'lar
go eng.StartWorkerWithOptions(ctx, "default", "worker-1", flow.WorkerOptions{Concurrency: 8})
```

`InstallN8NWorkflow` workflow'u içe aktarır, kaydeder ve aktifleştirir. Schedule ve polling trigger'ları kalıcı schedule'lara dönüşür; webhook, chat ve form trigger'larını `N8NHandler` karşılar. Workflow'u kaydetmeden yalnızca incelemek için `ImportN8NWorkflow` kullanın.

## Uyumluluk raporu

`ImportN8NWorkflow` bir rapor döndürür:
- `Runnable`: Tüm node ve özellikler destekleniyor.
- `Blocking`: Desteklenmeyenler.
- `Warnings`: Desteklenen ama dikkat gerektirenler.
- `Credentials`: Gereken credential'lar.
- `Triggers`: Workflow'un nasıl başlatılabileceği: manual, schedule (cron ile), poll, webhook, chat, form, subworkflow.

Bir marketplace yalnızca `Runnable` olan şablonları göstermeli ve listelenen credential'ları kullanıcıdan istemelidir.

## Execution başlatma

n8n'de olduğu gibi her execution'da tek bir trigger çalışır. Varsayılan trigger dışında birini seçmek için:

```go
eng.Execute(ctx, "default", "lead-intake", []byte(`{"__trigger": "Webhook", "items": [{"body": {"email": "a@b.c"}}]}`))
```

Sonuçları `flow.N8NOutputItems(execution.Output)` ile okuyun.

## HTTP endpoint'leri

| Endpoint | Amaç |
|---|---|
| `ANY /webhook/{path}` | Webhook trigger (`onReceived`, `lastNode`, `responseNode` modları) |
| `POST /webhook/{webhookId}` | Telegram / WhatsApp trigger'ları |
| `POST /chat/{workflowId}` | Chat trigger: `{"sessionId", "chatInput"}` → `{"output"}` |
| `POST /form/{workflowId}` | Form trigger: sıradaki sayfa (`page` + `resumeUrl`) veya `completion` döner |
| `GET/POST /resume/{executionId}?sig=…` | Bekleyen execution'ı devam ettirir: Wait, send-and-wait onayları, form sayfaları. Bağlantılar HMAC ile imzalanır. |

## Desteklenenler

- **Trigger'lar ve akış/veri node'ları:** IF, Switch, Merge, Loop, Code (JavaScript), Set ve diğerleri.
- **Entegrasyonlar:** HTTP Request, Telegram, Gmail, IMAP (polling), YouTube, Google Sheets/Drive/Calendar/Docs, Slack, Discord, WhatsApp, Notion, Airtable, WordPress, X, LinkedIn, Outlook, Postgres, MySQL, Redis, Supabase, SMTP.
- **AI:** Agent (tool, memory ve output parser'larla), LLM zincirleri; OpenAI, Anthropic, Gemini, OpenRouter, Ollama ve diğer modeller; desteklenen her entegrasyonun AI tool olarak kullanımı.
- **RAG:** Vektör veritabanları (In-Memory, Qdrant, Pinecone, Supabase).

Tam liste için [İngilizce dokümana](../n8n.md) bakın.

**Desteklenmeyenler:** Python Code, `require()`, eski binary .xls dosyaları, MCP node'ları, community node'lar.

## Güvenlik

İçe aktarılan workflow'lar güvenilmez kod olarak ele alınır:
- **Ağ:** Tüm dış bağlantılar `Outbound` politikasından geçer. Varsayılan olarak yalnızca public adreslere izin verilir (SSRF koruması).
- **JavaScript:** Kod; `require`, ağ ve dosya erişimi olmayan, zaman sınırlı bir sandbox'ta çalışır.
- **Ortam değişkenleri ve dosyalar:** `$env` ve dosya erişimi varsayılan olarak kapalıdır.
- **Resume bağlantıları:** HMAC ile imzalanır ve bekleyen node'a bağlıdır. Onay bağlantıları cevabı da imzalar; onay, e-posta tarayıcılarının bağlantıyı açmasıyla verilemesin diye bir onay sayfasındaki POST ile kaydedilir.
- **Credential'lar alan adına bağlıdır:** Bir şablon credential'larınızı kendi sunucusuna gönderemez. Bilinen servislerin credential'ları yalnızca kendi API alan adlarına gider. Herhangi bir credential'a `allowedDomains` ekleyerek bunu açıkça kısıtlayabilirsiniz.
- **Ayrı origin:** Güvenilmez workflow'lar kuracaksanız `N8NHandler`'ı ayrı bir origin'e (ör. `hooks.example.com`) mount edin. Workflow cevap gövdesini kontrol eder ancak cookie veya HSTS gibi header'ları ayarlayamaz.
- **Bellek:** JavaScript'in CPU süresi sınırlıdır, bellek kullanımı ise değildir (goja'nın bir kısıtı). Güvenilmez workflow'ları bellek sınırı olan ayrı worker süreçlerinde çalıştırın.

## Credential'lar

Credential'larınızı vault'tan çözmek için `flow.N8NCredentialProvider` arayüzünü uygulayın. OAuth2 credential'ları geçerli bir `accessToken` döndürmelidir; token yenileme provider'ın işidir. Google service account'ları otomatik olarak token'a çevrilir.

Birden fazla instance ile çalışırken `ChatMemory` ve `PollState` için kalıcı implementasyonlar sağlayın. `PollState` implementasyonunuz `N8NPollStateSwapper` (`CompareAndSwapPollState`) arayüzünü de sağlarsa cursor'lar atomik ilerler ve çakışan poll'ler aynı item'ları iki kez üretmez; aksi halde poll'ler yalnızca aynı süreç içinde sıraya alınır.
