# Solution

## 1. What was broken, and why

I found and fixed four main issues in the service:

### Mutex race in stats cache (`internal/stats/cache.go`)
- **What happened:** The `Record()` method was writing to the map and incrementing counter structs without acquiring a write lock (`c.mu.Lock()`). The reader `Get()` was using an `RLock`, but concurrent writes were causing silent data corruption and data races.
- **The Symptom:** This directly caused the stats counter to drift higher than the actual number of calls when multiple webhooks landed concurrently.
- **Fix:** Added `c.mu.Lock()` and `defer c.mu.Unlock()` at the start of `Record()`. Verified it using a concurrent test run with Go's `-race` flag.

### Request context leak in background goroutine (`internal/ingest/service.go`)
- **What happened:** The background goroutine processing the audio recordings (`processRecording`) was using the HTTP request context (`ctx`). Because we return a response to the provider immediately to keep ingestion fast, the HTTP handler terminates and cancels that context. When the goroutine tried to update the database, the query returned a cancelled context error, which was swallowed by a `// TODO` block.
- **The Symptom:** Call recordings were never getting marked as processed.
- **Fix:** Switched the background worker to use a clean context (`context.Background()`) so that the SQL update runs to completion even after the client disconnects.

### TOCTOU (check-then-act) race in deduplication (`internal/ingest/service.go` & database)
- **What happened:** The codebase used an application-level check (`EventExists`) followed by an insert. Two concurrent requests with the same `event_id` could run the check at the exact same time, see that the event doesn't exist, and proceed to insert duplicate rows. The `events` table only had a non-unique index on `event_id`, which allowed duplicate records to slip through.
- **The Symptom:** Double-counting stats on redelivered webhooks.
- **Fix:** Added a migration (`002_unique_event_id.sql`) to put a `UNIQUE` constraint on `events(event_id)`. Then, changed `InsertEvent` to use `INSERT ... ON CONFLICT (event_id) DO NOTHING` and return a boolean indicating whether a new row was actually created. We now exit early if it's a duplicate, eliminating the application-level check race entirely.

### Lost in-flight recording jobs during deployment (`cmd/server/main.go`)
- **What happened:** The service was launching background worker goroutines without keeping track of them. During a deployment (SIGTERM), `srv.Shutdown()` cleanly closed active HTTP connections, but the app immediately exited, killing any background recording jobs mid-flight.
- **The Symptom:** In-flight processing simply disappeared on deploy.
- **Fix:** Added a `sync.WaitGroup` to track active recording goroutines. Created a `Shutdown()` helper on the ingestion service that blocks until `wg.Wait()` finishes, and wired it up to run in `main.go` right after the server shuts down.

---

## 2. Choosing a Deduplication Strategy

I went with a database-level **Postgres UNIQUE constraint + ON CONFLICT DO NOTHING** approach.

- **Why this over Redis?** A Redis-based lock or cache (like `SET NX`) is great for speed, but Redis is not durable by default. If Redis restarts or gets flushed, we lose the deduplication history and could end up with duplicates in Postgres anyway. Postgres is the source of truth here, so the database-level constraint is the only way to guarantee absolute correctness.
- **Performance:** Since the database needs to index `event_id` anyway, adding a `UNIQUE` constraint doesn't add any extra overhead. It also saves us a database round-trip because we no longer need to run a separate `SELECT` check before calling `INSERT`.

For a production environment, we could use Redis as a fast, best-effort shield to catch duplicates early and save database CPU, but Postgres must remain the final authoritative gatekeeper.

---

## 3. Handling 10,000 Webhooks/Second

To scale this service up to 10k/sec, I would make the following changes:

1. **Move processing off the critical path:** Right now, we write to three database tables synchronously before returning a `200 OK`. At 10k/sec, this will quickly saturate the Postgres connection pool. Instead, the HTTP handler should only write the raw event payload to a fast message queue (like Kafka, AWS SQS, or Redis Streams) and immediately respond with a `200`. Dedicated worker pools can pull from the queue, write to the database, and handle recordings asynchronously.
2. **Batch database inserts:** Instead of writing every single event individually, workers should batch records (e.g., flush batches of 500 records or every 10ms) using `pgx.CopyFrom` or multi-row insert statements. This drastically reduces connection overhead and transaction commits.
3. **Switch the stats cache to Redis:** The in-memory cache we have now will diverge once we run multiple instances of the service behind a load balancer. It also gets wiped out on restarts. I would replace it with Redis counters using `HINCRBY` (e.g. `HINCRBY account:{id} call_count 1`). This is fast, shared across all replica instances, and stays persistent.
4. **Table partitioning:** A table receiving 10k inserts/sec will grow by nearly a billion records a month. I would partition the `events` and `calls` tables by time (e.g. daily or weekly partitions) to keep indexes small, speed up queries, and make archiving old data easy.
