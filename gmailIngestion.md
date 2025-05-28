
# Proposal: High‑Performance Gmail Ingestion Service (Go)


---

## 1  Background & Problem Statement
Gmail offers three ingestion paths (IMAP IDLE, REST polling, `watch()` + Pub/Sub). Each optimizes a different axis—latency, quota, or reachability—but none alone satisfies **all** of our SLA targets:

| Approach | Strength | Weakness |
|----------|----------|----------|
| IMAP IDLE | Push‑like latency | Workspace often disables IMAP; TCP long‑lived conn blocked by firewalls |
| REST Polling | Works everywhere | Burns quota & cannot meet < 5 s latency |
| watch() + Pub/Sub | Sub‑second latency, low quota | Missed pushes break `historyId` cursor |

Therefore, we design a **hybrid pipeline**: Push for speed, Poll for safety.

---

## 2  Summary
* **Push‑first** ensures sub‑second user experience.
* **Adaptive poll‑back** (15 min debounce) guarantees eventual consistency *without* constant chatter.
* **Quota‑saving tricks** (field masks, batchGet, idle‑skip) reduce read calls by **~95 %** versus naive polling.
* **Idempotent storage** + single‑thread‑per‑user syncing eliminates duplication and race conditions.

---

## 3  Goals & Success Criteria
| Metric | Target | Why it matters |
|--------|--------|----------------|
| **Latency** | P95 < 5 s | Conversations feel “live”; user stays in our UI instead of Gmail |
| **Consistency** | RPO < 15 min | Guarantees we never lose more than a coffee break of mail |
| **Quota** | &lt; 10 % per‑user cap | Leaves headroom for future features (send, modify) |
| **Scale** | 10 k accounts on 16‑CPU cluster | Matches 12‑month growth forecast with 2× buffer |
| **Ops** | Stateless, one‑click rollout | SRE can redeploy during business hours with zero downtime |

---

## 4  Why Push‑First & Poll‑Backed?
| Decision | What it does | Impact |
|----------|--------------|--------|
| **Use Gmail `watch()`** | Gmail pushes *deltas* to Pub/Sub when mailbox mutates | Latency < 1 s; 0 read quota for idle users |
| **Store `historyId` cursor** | Marks last processed change | Enables incremental fetch; avoids re‑scanning entire mailbox |
| **Poll after 15 min silence** | Fires `history.list` when pushes might have been dropped | Cures rare Pub/Sub loss; bounded extra reads |
| **Full poll every 3 h** | Deep sweep as safety net | Caps worst‑case drift at 3 h even if push system down |

---

## 5  System Architecture (Bird’s‑Eye)

```text
Gmail → Pub/Sub → gmailwatch —SyncJob→ gmailfetch → store (Postgres/S3)
                           ↑                               ↓
                     scheduler (timed jobs) ← cursor table / metrics
```

*Workers are **stateless** and **horizontally sharded**; scaling simply means `kubectl scale deployment/gmail-sync --replicas=N`.*

---

## 6  Component Design & Rationale

### 6.1 gmailwatch – Subscription & Event Intake
| Technique | What it does | Impact |
|-----------|--------------|--------|
| Renew `watch()` every 24 h | Gmail invalidates watches after 7 days | Prevents silent expiration |
| Verify Pub/Sub JWT | Confirms sender authenticity | Blocks spoofed pushes; compliance |
| *Ack after enqueue* | Only ack once job persisted | Guarantees at‑least‑once processing |

### 6.2 gmailfetch – Incremental Sync Engine
| Technique | What it does | Impact |
|-----------|--------------|--------|
| `users.history.list(startId)` | Pulls only **changed** messages | ~90 % fewer API calls than scanning `messages.list` |
| In‑memory dedup map | Skip duplicate msg IDs within a page window | Prevents wasted `messages.get` calls |
| **Selective `format=full` fetch** | Only fetch bodies for messages with attachments or when downstream flag set | Cuts response payload size by 20× for pure‑text mail |
| Batch `users.messages.get` (100 IDs) | One HTTP call for many IDs | Reduces TLS handshake/latency overhead |

### 6.3 store – Persistence Adapters
| Technique | What it does | Impact |
|-----------|--------------|--------|
| Single Postgres transaction per sync | Cursor update & data write are atomic | Eliminates “cursor advanced but rows missing” failure class |
| `ON CONFLICT DO NOTHING` UPSERT | Ignores duplicates | Idempotent re‑plays; simplifies error recovery |
| SHA‑256 attachment dedup | Detect identical blobs | Saves S3 storage and egress $ |

### 6.4 scheduler – Adaptive Poll Orchestrator
| Technique | What it does | Impact |
|-----------|--------------|--------|
| SQL query on `last_push_at` | Finds quiet accounts fast | No wasted job creation |
| Jittered run (+0–10 min) | Randomizes start times | Prevents spike that could throttle Gmail |
| Redis/SQS job fan‑out | Distributes jobs across pods | Horizontal scaling without leader election |

---

## 7  Data Models & Persistence Strategy
| Design Choice | What it does | Impact |
|---------------|-------------|--------|
| Separate `gmail_messages` vs `gmail_attachments` | Keep rows “skinny” | Faster metadata queries, smaller indexes |
| Cursor table with `SELECT … FOR UPDATE` | Locks one account’s cursor during sync | Guarantees only one worker advancing historyId |
| **Store MIME only in S3** | Offloads heavy blobs from DB | DB size stays constant; cheap cold storage |

---

## 8  Efficiency & Quota Preservation
Gmail enforces **10 million read requests per user/day** (project limit applies too). Below, each technique lists *how much* it saves:

| Technique | What it does | Typical Reduction |
|-----------|--------------|-------------------|
| **Field masks** (`fields=messages(id,threadId,internalDate,labelIds)`) | Strips 90 % of JSON keys | ~50 % bandwidth & CPU |
| **Batch Get (100 IDs)** | Coalesces 100 network trips into 1 | 99 % fewer HTTP requests |
| **Debounced polling (15 min)** | Poll only if no push | Reduces idle‑account calls from 96/day → < 4/day |
| **Idle account skip (last_msg > 30 d)** | Poll once a day | Saves 3.1 k calls/account/month |
| **Per‑user rate‑limiter (8 r/s)** | Token‑bucket gate | Hard cap prevents quota bursts from loops |

Collectively these keep average active‑user read volume around **5 %** of Google’s allowance.

---

## 9  Concurrency, Safety & Scaling
| Decision | What it does | Impact |
|----------|--------------|--------|
| **1 worker per user** | Serialize Gmail access per account | Zero race conditions; respects per‑user concurrent‑request limit (10) |
| **Stateless pods** | No in‑memory cursor state | Safe to restart/re‑deploy anytime |
| **HPA on queue depth** | Auto‑adds pods when load spikes | Maintains < 1 min queue SLAs |

---

## 10  Observability & Operations
| Signal | What it captures | Why it matters |
|--------|-----------------|----------------|
| **Log** `gmail_sync_complete` | user, source(push/poll), msg_count, duration | Root‑cause sync slowness per user |
| **Metric** `gmail_cursor_lag_seconds` | Now − internalDate of last ingested msg | Direct SLO indicator |
| **Trace** span `gmail.history.list` | Latency + error rate | Pinpoints Google API hot spots |

Prometheus alert **`cursor_lag_p95 > 7200`** (2 h) triggers page to investigate.

---

## 11  Edge‑Case Handling
| Scenario | Technique | Impact |
|----------|-----------|--------|
| Pub/Sub outage | Scheduler frequency ramps from 15 min → 5 min | Keeps lag < 15 min while push is down |
| `404 HistoryId` | Timestamp fallback `after:<last_ts>` | Auto‑recovers with ≤ 1 extra read pass |
| OAuth token revoked | Detect `401`, flip `accounts.status` → “disconnected” | Stops quota waste; surfaces UI warning |
| 25 MB attachment | Stream multipart upload | Memory stays < 10 MB even on big files |

---

## 12  Alternatives Ruled Out
| Option | Why Declined | Impact If Chosen |
|--------|--------------|------------------|
| **IMAP IDLE** | Disabled on many workspaces, firewall issues | High ops load; frequent disconnections |
| **Cloud Functions endpoint** | Ephemeral 540 s max runtime; opaque concurrency | Hard to batch DB writes; cost spikes |
| **Pure Polling** | Needs ≥1 call/min to hit 5 s SLA | Exceeds quota by 10× at 10 k accounts |
| **Kafka Bridge** | Adds Pub/Sub → Kafka hop | More infra to run; 30‑50 ms extra latency |
