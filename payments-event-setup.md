# Payment Events Setup — Design Decisions & Execution Log

**Project:** Digital Payments Event Streaming Infrastructure (POC)
**Author:** Caleb Daniels

This document records every design decision made while working through the Kafka payment events project, the reasoning behind each choice, and the commands/output used to implement and verify it. Sections are filled in incrementally as decisions are made.

---

## 1. Design Decisions

### 1.1 Topic Design

#### Topic Names & Domain Structure

The payment lifecycle (`initiated → authorised/unauthorised → validated/invalidated → completed/incomplete`) is modeled as **one topic per event type**, rather than one single "payment events" topic, so that downstream consumers can subscribe only to the event types they actually care about (e.g. a fraud consumer only needs `initiated` events; a notification consumer only needs the terminal outcomes) without filtering a mixed stream. Fraud scoring is modeled as a fully separate domain/topic from the payment lifecycle, since it is produced by an independent fraud producer/consumer pair (see `fraud.md`) rather than being a step the payment producer itself owns.

Naming convention: `<domain>.<entity>.<event-in-past-tense-or-adjective>`, lowercase, dot-separated. Positive/negative outcomes of the same step consistently use the same part of speech (`authorised`/`unauthorised`, `validated`/`invalidated`, `completed`/`incomplete`) rather than mixing verb and adjective forms.

**Payment domain topics** (`digitalpayments.payment.*`):

| Topic | Lifecycle step | Reasoning |
|---|---|---|
| `digitalpayments.payment.initiated` | Payment initiated | First step in the payment lifecycle; begins the entire process. |
| `digitalpayments.payment.authorised` | Payment authorised | Kept separate from `unauthorised` so downstream consumers that only care about payments that passed authorisation can subscribe to a single, filtered location instead of filtering a mixed stream themselves. |
| `digitalpayments.payment.unauthorised` | Payment not authorised | Failure outcome isolated to its own topic (e.g. for handling/investigating declined payments) without polluting the "happy path" topic. |
| `digitalpayments.payment.validated` | Payment validated | Separate step between authorisation and completion, per the brief's lifecycle. |
| `digitalpayments.payment.invalidated` | Payment invalidated | Failure outcome of validation, isolated the same way as `unauthorised`. |
| `digitalpayments.payment.completed` | Payment completed | Terminal success outcome. |
| `digitalpayments.payment.incomplete` | Payment not completed | Terminal failure outcome. A single downstream consumer (e.g. an event-store/reconciliation consumer) can subscribe to both `completed` and `incomplete` where the full history needs to be persisted, while other consumers only need one or the other. |

**Fraud domain topic:**

| Topic | Reasoning |
|---|---|
| `digitalpayments.fraud.scored` | Fraud scoring (low/medium/high) is produced by an independent fraud producer that consumes `payment.initiated` events — it is not a step the payment producer itself performs, so it is kept in its own domain namespace rather than under `payment.*`. |

**Cross-topic ordering / audit strategy:** Since a single payment's events are spread across multiple topics, Kafka's within-partition ordering guarantee alone isn't enough to reconstruct a payment's full journey. Every event payload will carry:
- a stable **`paymentId`** — also used as the Kafka **message key**, so all events for one payment land on the same partition number within each topic;
- an **event-created timestamp**;
- a **previous-source reference field** (pointing to the event that caused this one).

Downstream consumers (event stores, reconciliation/audit systems) reconstruct the full lifecycle by grouping on `paymentId` first, then ordering by timestamp/causation reference within that group — rather than relying on cross-topic ordering, which Kafka does not provide.

**Notification domain topic:** `digitalpayments.notification.processed` — designed after the initial pass above; full detail (including schema) in `notification.md`. Kept as a single topic rather than split by outcome, since it's a flat terminal record with no further lifecycle, unlike the payment domain's multi-step topics. 10 partitions, RF=3, `min.insync.replicas=2`, `cleanup.policy=delete`, 7-day + 100GB/partition retention, `lz4` compression — consistent with the rest of the system (see `notification.md` for why nothing about this topic's volume/criticality argued for differing from precedent). It captures a dimension the payment topics can't: whether the *notification itself* was successfully delivered (`notificationOutcome`), independent of whether the payment (`paymentOutcome`) succeeded.

#### Topic Configuration Policy Scope

Rather than reasoning through partitions, replication factor, retention, cleanup policy, compression, and min in-sync replicas independently for all 8 topics, a **shared configuration policy** is applied to all `digitalpayments.payment.*` topics (they represent the same entity, the same audit/compliance retention needs, and similar throughput). The fraud topic (`digitalpayments.fraud.scored`) is reasoned through separately, since its access pattern and audit requirements may differ.

#### Partitions

**`digitalpayments.payment.initiated`, `.authorised`, `.validated`, `.completed` (happy-path topics): 10 partitions each.**
At the stated peak throughput of 1000 msg/sec (project.md line 11), 10 partitions works out to ~100 msg/sec per partition — comfortably within a single Kafka partition's throughput capacity. 10 is deliberately above the currently-known number of consumer instances needed, to leave headroom for known predictable spikes (e.g. pay-day influx) without needing to increase partition count later. This matters because the partition key is `paymentId`: increasing partition count later changes the key→partition hash mapping for events published from that point forward, which would break the "same payment always lands on the same partition" guarantee that the per-payment ordering/reconciliation strategy depends on. Provisioning generously once, rather than growing incrementally, avoids that disruptive rebalancing.

**`digitalpayments.payment.unauthorised`, `.invalidated`, `.incomplete` (failure-path topics): 5 partitions each.**
These topics only carry the failure fraction of total volume, which is expected to be meaningfully lower than the happy-path volume, so fewer partitions are sufficient for both throughput and consumer parallelism.

**`digitalpayments.fraud.scored`: 10 partitions.**
Every payment that is initiated is scored for fraud — unlike the happy-path payment topics, this topic sees 100% of the ~1000 msg/sec initiation volume, not a filtered subset. It also feeds the tightest lag SLA in the system (<50ms fraud detection, project.md line 14), so it is provisioned the same as the happy-path topics rather than treated as a lower-volume topic.

#### Replication Factor

**All topics (both `digitalpayments.payment.*` and `digitalpayments.fraud.scored`): replication factor 3.**
The cluster has exactly 3 brokers, so 3 is the practical ceiling — every partition gets a full copy on every broker. This means the cluster can tolerate any single broker failure without losing data (the remaining brokers each still hold a complete copy). A uniform replication factor is used across all topics — including the notification topics still to be designed — to eliminate any possibility of data loss across the system, even though the explicit "zero data loss" requirement (project.md line 16) only names fraud and payment events specifically. This is a deliberate simplification: uniform configuration is simpler to operate for a POC, at the cost of applying the same storage/replication overhead to topics that may not strictly require it.

Note: replication factor only determines how many copies of a partition *exist*. It is distinct from **min in-sync replicas** (a topic-level setting, addressed below) and **producer `acks`** (a producer-level setting, addressed in Section 1.2) — both of which determine how many of those copies must acknowledge a write before it is considered durable.

#### Retention

**All topics (happy-path, failure-path, and fraud): time-based 7 days + size-based 100GB per partition.**

The 7-year regulatory retention requirement (project.md line 16) is met by a **downstream AWS database**, not by Kafka itself — holding 7 years of data directly on Kafka brokers at 1000 msg/sec would be prohibitively expensive and isn't what Kafka is designed for. Kafka's retention only needs to hold data long enough to guarantee the downstream persistence consumer can never fall behind and lose data before it's committed to that long-term store.

- **Time-based (`retention.ms` = 7 days):** gives a full week of buffer for the downstream persistence consumer, well beyond what should ever be needed if that consumer is healthy.
- **Size-based (`retention.bytes` = 100GB, per partition):** acts as a safety net against disk exhaustion from an abnormal spike, and is deliberately set *above* expected 7-day volume rather than below it, so it isn't the limit that actually triggers deletion under normal conditions. Kafka applies size- and time-based retention together as "whichever limit is hit first," so an undersized `retention.bytes` value would silently override the intended 7-day window — this was checked explicitly:
  - `retention.bytes` in Kafka is a **per-partition** setting, not per-topic.
  - Happy-path/fraud topics run 10 partitions at the stated 1000 msg/sec peak → ~100 msg/sec per partition.
  - Using a rough ~1KB/message estimate (to be revisited once the schema is finalized): ~100KB/sec per partition.
  - 7 days × 100KB/sec ≈ **60GB per partition** just to hold one week of data at expected average volume, with zero headroom.
  - **100GB** was chosen to sit meaningfully above that 60GB baseline, so it only kicks in for genuinely abnormal growth (e.g. a retry storm or sustained spike well above the 1000 msg/sec design target), not normal operation.
  - The 5-partition failure-path topics (`unauthorised`, `invalidated`, `incomplete`) carry a fraction of total volume, so 100GB is comfortably oversized headroom there too — applying the same uniform value keeps configuration simple without under-provisioning any topic.

Noted for later: this may be simplified to time-based-only retention once real volume data is available and the size-based safety net proves unnecessary in practice.

#### Cleanup Policy

**All topics: `delete` (not `compact`).**

These topics are immutable event streams, not state stores — each message is a fact about something that happened (a payment was initiated, authorised, scored, etc.), and the goal is to preserve the full lifecycle history for replay, traceability, and audit. Log compaction's model ("only the latest value per key matters, everything older can be discarded") is designed for state stores where old values are genuinely obsolete once superseded — that doesn't fit an audit trail, where even a duplicate or retried publish for the same `paymentId` should remain visible rather than being silently collapsed away. `delete` (governed by the time/size retention configured above) correctly ages out data based on how long it's been retained, not on whether a "newer" value for the same key exists. Deletion via this policy also feeds cleanly into the downstream AWS persistence layer building a full audit history per payment.

#### Compression

**All topics: `lz4`.**

Compression happens synchronously in the producer's send path and decompression in the consumer's read path — both cost CPU time and eat directly into the lag budget. Given the tightest SLA in the system is fraud detection at <50ms (project.md line 14), speed of compression/decompression takes priority over maximum compression ratio. Between `lz4` and `snappy` (both low-CPU, fast options), `lz4` was chosen because it's faster while offering a very similar compression ratio to `snappy` — `gzip` and `zstd` were ruled out despite better ratios, since the extra CPU cost isn't justified when speed is the primary driver and message payloads are small JSON events rather than large blobs where ratio gains would matter more.

#### Min In-Sync Replicas

**All topics: `min.insync.replicas=2`.**

With replication factor 3, requiring 2 in-sync replicas means a write is only considered durable once it exists on the leader plus at least one follower — the cluster tolerates a single broker outage without blocking writes or losing acknowledged data, since the message has already been replicated to a second broker before being acknowledged. Requiring all 3 (`min.insync.replicas=3`) would block writes entirely the moment any one broker goes down, which is too strict given the throughput/lag requirements; requiring only 1 would allow an acknowledged write to exist on just the leader, risking loss if that broker fails before replication completes. 2 is the standard balance for RF=3. Note: the cluster's brokers already default to `min.insync.replicas=2` — this will still be set explicitly in the topic-creation command so the design decision is visible rather than relying on an implicit default.

#### Message Schema

**Example — a message published to `digitalpayments.payment.authorised` (i.e. after passing through `initiated` and `fraud.scored`):**

```json
{
  "processingState": "digitalpayments.payment.authorised",
  "retryCount": 0,
  "previousSource": "digitalpayments.fraud.scored",
  "createdDateTime": "2026-09-27T00:00:00.000Z",
  "transaction": {
    "paymentId": "pay-01",
    "payerAccountNumber": "1234567890",
    "payerAccountType": "SVGS",
    "amount": 100.75,
    "payeeAccountNumber": "9876543210",
    "payeeAccountType": "SVGS",
    "status": "Pending",
    "fraudScore": "Low"
  }
}
```

**Field reference:**

| Field | Type | Required? | Applies to | Description |
|---|---|---|---|---|
| `processingState` | string (topic name) | Always | All events | The topic this message is being published to — identifies the exact pipeline stage. |
| `retryCount` | integer | Always | All events | Number of producer retry attempts for this message, for traceability into idempotency/error-handling behaviour. |
| `previousSource` | string (topic name) | Always | All events except `initiated` | The topic of the event that caused this one to be published — used with `createdDateTime` and `paymentId` by downstream consumers to reconstruct a payment's full lifecycle across topics, since Kafka does not guarantee cross-topic ordering. |
| `createdDateTime` | ISO-8601 timestamp | Always | All events | When this event was created; used for ordering events for a given `paymentId` downstream. |
| `transaction.paymentId` | string | Always | All events | Correlation identifier for the payment; also used as the Kafka message key, so all events for one payment land on the same partition number per topic. |
| `transaction.payerAccountNumber` | string (10-digit) | Always | All events | The payer's account number. **PII** — kept raw/unmasked for this POC only, using mock account numbers rather than real client data; in a production system this would be tokenized or masked before entering Kafka. |
| `transaction.payerAccountType` | string | Always | All events | The payer's account type (e.g. `SVGS` = savings). |
| `transaction.payeeAccountNumber` | string (10-digit) | Always | All events | The counterparty/payee's account number. Same PII treatment as `payerAccountNumber`. |
| `transaction.payeeAccountType` | string | Always | All events | The payee's account type. |
| `transaction.amount` | decimal (2 d.p.) | Always | All events | Transaction amount. Kept as a decimal value rather than integer minor-units for this design. |
| `transaction.status` | enum: `Pending` \| `Completed` \| `Failed` | Always | All events | Coarse business outcome of the payment — `Pending` from `initiated` through `validated`, `Completed` once it reaches the `completed` topic, `Failed` if it lands on any failure-path topic (`unauthorised`, `invalidated`, `incomplete`). Distinct from `processingState`, which tracks the fine-grained pipeline stage rather than the business outcome. |
| `transaction.fraudScore` | enum: `Low` \| `Medium` \| `High` | Conditional — present from `fraud.scored` onward, absent on `initiated` | All events after fraud scoring has occurred | The fraud score assigned by the fraud producer/consumer pipeline, carried forward into the payment lifecycle events so downstream consumers of payment topics don't need to separately join against `digitalpayments.fraud.scored`. |

**Mandatory fields across every topic:** `processingState`, `retryCount`, `paymentId`, account number/type (payer and payee), and `amount`.

<!-- Serialization format (JSON vs Avro vs Protobuf) is addressed as a producer design decision in Section 1.2, since it concerns encoding rather than the field-level data model. -->

### 1.2 Producer Design

#### Event Types

Payment producer publishes: `initiated`, `authorised`/`unauthorised`, `validated`/`invalidated`, `completed`/`incomplete`. Fraud producer (independent pipeline, consumes `initiated`) publishes: `fraud.scored`. Notification producer (independent pipeline, triggered by payment outcome topics) publishes: `notification.processed`. See Section 1.1 for full topic list and reasoning, and `notification.md` for the notification producer's specific decisions (all consistent with this section's precedent — see `notification.md`).

#### Partition Key Strategy

**Key = `paymentId`.** Ensures every event for a given payment lands on the same partition number within each topic (Kafka's hash-based partition assignment is deterministic per key), which is what the cross-topic ordering/reconstruction strategy (timestamp + `previousSource` + `paymentId` grouping) depends on.

#### Durability (`acks`)

**`acks=all` for all payment and fraud topics.**

`min.insync.replicas` only has an effect when the producer uses `acks=all` — with `acks=1`, the producer is acknowledged as soon as the partition leader writes the message locally, before any follower replicates it, which would make the earlier `min.insync.replicas=2` decision ineffective (a leader failure immediately after acking could still lose an already-acknowledged message). Given project.md line 16's explicit **zero data loss** requirement for fraud and payment events, `acks=all` is required to actually deliver on that guarantee — the producer only receives success once the message exists on the leader plus at least one in-sync follower. This is a deliberate trade-off: `acks=all` is the slowest of the three `acks` options, but for this POC, correctness (no data loss) takes priority over raw producer speed.

#### Idempotency

**`enable.idempotence=true` on the producer.**

With `acks=all`, an acknowledgment lost in transit before reaching the producer would otherwise trigger a retry that could write a duplicate message even though the original send actually succeeded. `enable.idempotence=true` assigns the producer a producer ID and per-partition sequence numbers, letting the broker detect and discard duplicates caused by producer-level retries.

This is distinct from the `retryCount` field in the message schema, which is not involved in idempotency — it exists purely to help distinguish between multiple payloads sharing the same `paymentId` within the same topic (e.g. for traceability), not to deduplicate. Kafka's idempotent producer only protects against a retry of the *same* send; it does not protect against a genuinely separate duplicate business-level submission (e.g. the same payment being initiated twice by an upstream caller) — that class of duplicate is a business/application-level concern, out of scope for the Kafka producer configuration itself.

#### Serialization Format

**JSON.**

For this POC, readability and ease of debugging (e.g. inspecting raw output via `kafka-console-consumer.sh` without needing a schema registry or decoder tooling) outweighs the compactness and schema-evolution benefits of a binary format like Avro or Protobuf. The trade-off — larger message size and slower (de)serialization — is accepted deliberately, since JSON also makes downstream investigations/audits easier to reason about directly from the raw event payload.

#### Error Handling & Retry Strategy

**`retries=3`, `retry.backoff.ms=10`, `delivery.timeout.ms=50`. On exhausted retries: flag for manual intervention.**

Payment initiation is synchronous from the customer's perspective, so the producer can't wait indefinitely for Kafka to recover — it needs a bounded retry window that still fits inside the overall <150ms payment lag target (project.md line 13). 3 retries with a 10ms backoff between attempts requires at least 20ms just for the backoff delays, so `delivery.timeout.ms` (the overall ceiling across all attempts and backoffs) must be comfortably larger than that — 50ms was chosen to leave ~30ms of actual attempt/processing time on top of the 20ms backoff, while still leaving the majority of the 150ms end-to-end budget for everything downstream of the producer (fraud scoring, validation, completion).

If all retries are exhausted and the send still fails, the payment is **flagged for manual intervention** rather than silently dropped or automatically retried indefinitely — consistent with the audit/zero-data-loss posture established for topic and durability design; a payment event should never simply disappear without a trace.

#### Production Scale

**10 mock payment journeys**, comprising:
- 7 fully successful (`initiated` → `fraud.scored` → `authorised` → `validated` → `completed`)
- 1 authorisation failure (`initiated` → `fraud.scored` → `unauthorised`)
- 1 validation failure (`initiated` → `fraud.scored` → `authorised` → `invalidated`)
- 1 completion failure (`initiated` → `fraud.scored` → `authorised` → `validated` → `incomplete`)

This distribution exercises every payment-domain topic — including all three failure-path topics — at least once, while the majority still follow the happy path, matching realistic real-world outcome proportions. Account numbers and amounts are randomised per payment; account type is defaulted to `SVGS` for all mock data.

**Generation mechanism:** `kafka-console-producer.sh` is used as the actual delivery mechanism (rather than a custom Kafka client), but a small helper script generates the randomised JSON payloads and pipes them in — parameterised by target topic and message count — rather than typing ~40-50 individually correlated messages (matching `paymentId`, `previousSource` chains, and timestamps across each payment's journey) by hand.

### 1.3 Consumer Design

Three independent consumer groups are designed, representing three distinct downstream systems:

1. **Fraud consumer** — consumes `initiated` events, runs fraud scoring, feeds the `fraud.scored` topic.
2. **Notification consumer** — consumes payment outcome events (validated/failed), sends customer-facing notifications.
3. **Reconciliation/audit consumer** — consumes every payment and fraud event, and is the mechanism that implements the 7-year regulatory retention requirement (project.md line 16) by persisting the full event history downstream (to an AWS database) beyond Kafka's own much shorter 7-day retention window. The actual AWS persistence implementation is out of scope for this POC — only the consumer design itself is covered here.

#### Consumer 1: Fraud Consumer

| Aspect | Decision | Reasoning |
|---|---|---|
| **Purpose & SLA** | Consumes `digitalpayments.payment.initiated`, runs fraud scoring, produces to `digitalpayments.fraud.scored`. | Tightest lag SLA in the system: <50ms (project.md line 14). |
| **Group name** | `fraud-scoring-service` | Hyphen-separated, per the brief's stated consumer group naming convention (project.md line 170) — distinct from the dot-separated topic naming convention. |
| **Event filtering** | Consumes all `initiated` events, no exceptions. | Every initiated payment must be scored for fraud; there's no subset that can be skipped. |
| **Processing logic** | Allocates a fraud score (`Low`/`Medium`/`High`) per payment, then produces the result to `fraud.scored`. | Matches the fraud producer's defined output (project.md lines 34-35). |
| **Offset management** | Manual commit, **after** successful processing and successful produce to `fraud.scored` (not before). | Committing before processing risks losing that message's work entirely on a crash (never retried). Committing after processing means a crash between "produced" and "commit" causes redelivery on restart — a duplicate, not a loss — which is the safer failure mode of the two, at the cost of possible duplicates (see below). |
| **Ordering & isolation** | **At-least-once processing** (not exactly-once). | True exactly-once across a consume-then-produce pipeline requires Kafka's transactional API (atomically tying the offset commit to the `fraud.scored` produce, plus `isolation.level=read_committed` on downstream consumers) — that adds real coordination latency, which directly conflicts with the <50ms SLA. At-least-once accepts that a crash could cause a duplicate `fraud.scored` event for the same payment; protection against that duplicate is pushed downstream (the producer's `enable.idempotence=true` already prevents duplicate *broker-level* writes from producer retries — see Section 1.2 — and any consumer of `fraud.scored` can deduplicate on `paymentId` if needed) rather than paid for on the tightest-latency hop in the system. |
| **Scalability** | Runs as multiple parallel consumer instances within the group. | `initiated` has 10 partitions (Section 1.1), so up to 10 instances can run in parallel — needed to keep up with the same throughput/spike headroom reasoning used when sizing that topic's partitions. |

#### Consumer 2: Notification Consumer

| Aspect | Decision | Reasoning |
|---|---|---|
| **Purpose & SLA** | Consumes payment outcome events and sends real-time customer notifications. | Lag target: <2s (project.md line 15) — the most relaxed SLA of the three consumers. |
| **Group name** | `payment-notification-service` | Hyphen-separated, per convention. |
| **Event filtering** | Subscribes to all three failure-path topics (`unauthorised`, `invalidated`, `incomplete`) — triggering a "declined" notification — plus `completed` only (not `validated`) — triggering an "approved" notification. | The customer should only be notified once, at the final outcome, not at intermediate stages like validation. This also matches the notification producer's stated output of "payment approved or failed" events (project.md line 42). |
| **Processing logic** | Formats and sends (mocked, for this POC) a customer-facing notification based on the outcome. | — |
| **Offset management** | Manual commit, after successful processing — same mechanism as the fraud consumer. | — |
| **Ordering & isolation** | At-least-once processing, with duplicate-notification prevention pushed into the notification-sending logic itself: dedup keyed on `paymentId` + `payerAccountNumber` + outcome before actually sending. | Unlike a duplicate `fraud.scored` event (invisible to the customer), a duplicate "payment declined"/"payment approved" notification is customer-visible and would be noticed — so even though the SLA here is looser (2s, more room for transactional overhead than the fraud consumer's 50ms), the dedup is still handled application-side rather than via Kafka transactions, keeping the design consistent with the rest of the system. |
| **Scalability** | 20 parallel consumer instances. | This group subscribes to **4 topics**, not just one: `unauthorised` (5 partitions) + `invalidated` (5) + `incomplete` (5) + `completed` (10) = 25 partitions total — a single consumer group's instances are distributed across *all* of its subscribed topics' partitions combined, not just the largest one. 20 is a deliberate choice below that 25-partition ceiling. |

#### Consumer 3: Reconciliation/Audit Consumer

| Aspect | Decision | Reasoning |
|---|---|---|
| **Purpose & SLA** | Consumes every payment and fraud event and persists them downstream (AWS database, out of scope for this POC), implementing the 7-year regulatory retention requirement (project.md line 16) beyond Kafka's own 7-day window. Lag target: near real-time, **<1 minute**. | The most relaxed SLA of the three consumers, but still needs enough margin that a slow period could never risk data aging out of Kafka's 7-day retention before this consumer has durably persisted it. |
| **Group name** | `digitalpayments-payment-retention-service` | Hyphen-separated throughout, per convention. |
| **Event filtering** | All 9 topics — every payment-domain topic, `fraud.scored`, and `notification.processed` — with no exclusions. | The audit trail needs to be complete; no event type can be legitimately skipped. (Updated from 8 to 9 topics once the notification topic was designed — see `notification.md`.) |
| **Processing logic** | Persists each event, using `paymentId` (grouping), `createdDateTime` (ordering within a payment), and `previousSource` (verifying the causal chain) to reconstruct and validate each payment's full lifecycle. | Directly implements the cross-topic ordering/reconstruction strategy defined in Section 1.1, since Kafka provides no ordering guarantee across the 9 separate topics a single payment's events are spread across. |
| **Offset management** | Manual commit, after the downstream persistence write succeeds. | Same "commit only after the durable action is confirmed" pattern as the other two consumers — committing before the write succeeds risks losing an event's audit record entirely on a crash. |
| **Ordering & isolation** | At-least-once processing, with duplicate detection at the storage layer keyed on **`paymentId` + topic name**. | A duplicate audit record is a real data-quality problem for this consumer (unlike the fraud consumer's invisible duplicates), so it isn't left unhandled — but true exactly-once/Kafka transactions aren't needed either, since a simple idempotent key is sufficient: under this design, a given payment produces at most one message per topic, so `paymentId` + topic uniquely identifies a record. Partition number was considered but adds no extra discriminating power, since it's already deterministically derived from `paymentId` as the partition key. |
| **Scalability** | 20 parallel consumer instances. | This group subscribes to all 9 topics, totaling 75 partitions (6 topics × 10 partitions + 3 topics × 5 partitions). 20 is deliberately below that ceiling — consistent with this being the least latency-sensitive of the three consumers (<1 minute vs. <2s and <50ms), it can run with fewer instances than its theoretical maximum parallelism. |

---

## 2. Topic Creation

**Cluster:** local 3-broker Kafka cluster (KRaft mode, no separate Zookeeper) running under Rancher Desktop's built-in Kubernetes (`kafka-0`, `kafka-1`, `kafka-2` pods, plus a `kafka-ui` pod), accessed via `kubectl exec kafka-0 -- <kafka CLI command> --bootstrap-server localhost:9092` (the Kafka CLI binaries — `kafka-topics`, `kafka-console-producer`, etc. — live directly on the broker image's `PATH`, no `.sh` suffix on this image).

**Pre-existing state found in the cluster before this session's changes:** `digitalpayments.fraud.scored`, `digitalpayments.payment.authorised`, `digitalpayments.payment.validated`, and `digitalpayments.payment.completed` already existed with 10 partitions / RF=3 / `min.insync.replicas=2` / `cleanup.policy=delete` / `retention.ms=604800000` (7 days) — matching this design almost exactly, but without `retention.bytes` set. `digitalpayment.payment.initiated` (missing the "s") also existed with the same config, under the wrong name. `digitalpayments.payments.lifecycle` and `my-topic` also existed as unrelated scratch topics. Decision: leave all pre-existing topics untouched for now (including the `retention.bytes` gap on the four correctly-named ones); the typo'd topic will be deleted and recreated separately; the two scratch topics are left alone as they don't interfere with this design.

**Commands used to create the three missing failure-path topics** (`digitalpayments.payment.unauthorised`, `.invalidated`, `.incomplete` — 5 partitions each per the partitioning design in Section 1.1):

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --create \
  --topic digitalpayments.payment.unauthorised \
  --partitions 5 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config retention.bytes=107374182400

kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --create \
  --topic digitalpayments.payment.invalidated \
  --partitions 5 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config retention.bytes=107374182400

kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --create \
  --topic digitalpayments.payment.incomplete \
  --partitions 5 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config retention.bytes=107374182400
```

(`retention.bytes=107374182400` = 100GB, per the retention design in Section 1.1.)

**Verification (`--describe` output):**

```
Topic: digitalpayments.payment.unauthorised	TopicId: K4cxDlcKSJOAnzinyki3wQ	PartitionCount: 5	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000,retention.bytes=107374182400
	Partition: 0	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Partition: 1	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Partition: 2	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Partition: 3	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Partition: 4	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1

Topic: digitalpayments.payment.invalidated	TopicId: Z-Qf8sA9QVuz4jfJHzASjQ	PartitionCount: 5	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000,retention.bytes=107374182400
	Partition: 0	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Partition: 1	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Partition: 2	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Partition: 3	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Partition: 4	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1

Topic: digitalpayments.payment.incomplete	TopicId: nJxAA94MSLqTkTE1EzhNhg	PartitionCount: 5	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000,retention.bytes=107374182400
	Partition: 0	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Partition: 1	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Partition: 2	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Partition: 3	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Partition: 4	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
```

All three created with the intended config and evenly distributed leadership across all 3 brokers.

<!-- digitalpayments.payment.initiated to be added once the typo'd digitalpayment.payment.initiated topic is deleted/recreated -->

---

## 3. Producer Setup

### Producer Commands

Messages are generated and sent via [scripts/generate_payment_events.py](scripts/generate_payment_events.py), which builds the JSON payload per the schema in Section 1.1 and pipes it into `kafka-console-producer` (running inside the `kafka-0` pod, invoked via `kubectl exec -i`) with `parse.key=true` / `key.separator=:` so the Kafka message key is `paymentId`. One invocation per topic, in pipeline order:

```bash
python generate_payment_events.py --topic digitalpayments.payment.initiated   --count 10
python generate_payment_events.py --topic digitalpayments.fraud.scored        --count 10
python generate_payment_events.py --topic digitalpayments.payment.authorised  --count 9
python generate_payment_events.py --topic digitalpayments.payment.unauthorised --count 1
python generate_payment_events.py --topic digitalpayments.payment.validated   --count 8
python generate_payment_events.py --topic digitalpayments.payment.invalidated --count 1
python generate_payment_events.py --topic digitalpayments.payment.completed   --count 7
python generate_payment_events.py --topic digitalpayments.payment.incomplete  --count 1
```

Internally, the first call creates 10 new mock payments and assigns each one an outcome bucket (7 success / 1 auth-fail / 1 validation-fail / 1 completion-fail); every later call advances the existing payments that are eligible for that topic (correct outcome bucket + correct current stage) using a local state file (`scripts/payment_state.json`), reusing each payment's `paymentId`, accounts, amount and fraud score so the fields stay correlated across topics.

Every call was dry-run tested first (`--dry-run` flag, prints the generated messages without sending) to verify the pipeline chained correctly before actually producing to the cluster.

### Mock Events Produced

**47 total messages across 10 complete payment journeys:**

| Payment | Outcome | Journey (topics, in order) |
|---|---|---|
| pay-001 | auth_fail | `initiated` → `fraud.scored` → `unauthorised` |
| pay-002 | success | `initiated` → `fraud.scored` → `authorised` → `validated` → `completed` |
| pay-003 | validation_fail | `initiated` → `fraud.scored` → `authorised` → `invalidated` |
| pay-004 | completion_fail | `initiated` → `fraud.scored` → `authorised` → `validated` → `incomplete` |
| pay-005, 006, 007, 008, 009, 010 | success | `initiated` → `fraud.scored` → `authorised` → `validated` → `completed` |

### Example Events (2+ different event types)

**`digitalpayments.payment.initiated`** (pay-001, before fraud scoring — no `fraudScore` field, no `previousSource`):
```json
{"processingState":"digitalpayments.payment.initiated","retryCount":0,"createdDateTime":"2026-09-27T18:39:16.407Z","transaction":{"paymentId":"pay-001","payerAccountNumber":"0993152446","payerAccountType":"SVGS","amount":4428.33,"payeeAccountNumber":"3753539859","payeeAccountType":"SVGS","status":"Pending"}}
```

**`digitalpayments.fraud.scored`** (pay-001 — same payment, now carrying a `High` fraud score which will cause it to diverge to `unauthorised` next):
```json
{"processingState":"digitalpayments.fraud.scored","retryCount":0,"previousSource":"digitalpayments.payment.initiated","createdDateTime":"2026-09-27T18:39:41.104Z","transaction":{"paymentId":"pay-001","payerAccountNumber":"0993152446","payerAccountType":"SVGS","amount":4428.33,"payeeAccountNumber":"3753539859","payeeAccountType":"SVGS","status":"Pending","fraudScore":"High"}}
```

**`digitalpayments.payment.completed`** (pay-002 — a full success journey reaching its terminal state):
```json
{"processingState":"digitalpayments.payment.completed","retryCount":0,"previousSource":"digitalpayments.payment.validated","createdDateTime":"2026-09-27T18:41:11.389Z","transaction":{"paymentId":"pay-002","payerAccountNumber":"8275162884","payerAccountType":"SVGS","amount":1169.9,"payeeAccountNumber":"1848434320","payeeAccountType":"SVGS","status":"Completed","fraudScore":"Medium"}}
```

### Evidence of Successful Production (Offsets & Partition Assignments)

Verified via `kafka-console-consumer --from-beginning --property print.partition=true --property print.offset=true --property print.key=true`.

**`digitalpayments.payment.initiated`** (10 messages spread across 8 of 10 partitions):
```
Partition:7 | Offset:0 | pay-003
Partition:7 | Offset:1 | pay-008
Partition:5 | Offset:0 | pay-004
Partition:4 | Offset:0 | pay-005
Partition:4 | Offset:1 | pay-009
Partition:3 | Offset:0 | pay-002
Partition:3 | Offset:1 | pay-007
Partition:2 | Offset:0 | pay-006
Partition:1 | Offset:0 | pay-001
Partition:0 | Offset:0 | pay-010
```

**`digitalpayments.fraud.scored`** (same 10 payments — note `pay-001` lands on **partition 1 in this topic too**, matching its partition in `initiated` above; both topics have 10 partitions, so the same key hashes to the same partition number in each):
```
Partition:3 | Offset:0 | pay-002
Partition:3 | Offset:1 | pay-007
Partition:0 | Offset:0 | pay-010
Partition:7 | Offset:0 | pay-003
Partition:7 | Offset:1 | pay-008
Partition:4 | Offset:0 | pay-005
Partition:4 | Offset:1 | pay-009
Partition:5 | Offset:0 | pay-004
Partition:2 | Offset:0 | pay-006
Partition:1 | Offset:0 | pay-001
```

**`digitalpayments.payment.completed`** (7 success-path payments — `pay-002` again lands on partition 3, consistent with `initiated`/`fraud.scored`):
```
Partition:7 | Offset:0 | pay-008
Partition:0 | Offset:0 | pay-010
Partition:4 | Offset:0 | pay-005
Partition:4 | Offset:1 | pay-009
Partition:3 | Offset:0 | pay-002
Partition:3 | Offset:1 | pay-007
Partition:2 | Offset:0 | pay-006
```

### Addendum: Notification Producer

After the notification topic/producer were designed (see `notification.md`), a second small script ([scripts/generate_notification_events.py](scripts/generate_notification_events.py)) produced the 10 `digitalpayments.notification.processed` events — one per payment, derived from each payment's actual terminal stage in `payment_state.json` rather than re-randomizing anything, so `paymentOutcome`/`previousSource`/`fraudScore` stay consistent with each payment's real journey. One payment (`pay-005`, a `completed`/`Approved` journey) was deliberately given `notificationOutcome: false` to demonstrate that notification delivery success is genuinely independent of payment outcome — the whole reason this topic isn't redundant with `payment.completed`:

```json
{"processingState":"digitalpayments.notification.processed","retryCount":0,"previousSource":"digitalpayments.payment.completed","createdDateTime":"2026-09-27T20:09:40.520Z","paymentOutcome":"Approved","notificationOutcome":false,"transaction":{"paymentId":"pay-005","status":"Completed","fraudScore":"Low"}}
```

### Decisions Made During Testing

- Every stage was dry-run tested before sending for real, which caught a cosmetic JSON field-ordering issue (`previousSource` landing after `transaction`) before any real messages were sent.
- The first attempt to verify `fraud.scored` via `kafka-console-consumer` with `--timeout-ms 5000` returned zero messages despite 10 having been sent — a new consumer group's initial join/partition-assignment can take longer than 5 seconds, especially routed through `kubectl exec`. Increasing the timeout to `10000ms` resolved it (see Section 7).

---

## 4. Consumer Groups

For this POC, each designed consumer group is instantiated with `kafka-console-consumer --group <name>` as a stand-in for the real service (fraud scoring model, notification sender, AWS persistence job) — the `--group` flag gives it real consumer-group semantics (partition assignment, offset tracking, rebalancing) even though the "processing" itself is just printing the message. One caveat worth being explicit about: `kafka-console-consumer` auto-commits offsets by default, whereas the design in Section 1.3 specifies manual commit *after* successful downstream processing for all three groups — that distinction only matters once a real client application replaces the CLI tool, so it's noted here rather than hidden.

### Fraud Consumer (`fraud-scoring-service`)

```bash
kubectl exec kafka-0 -- kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic digitalpayments.payment.initiated \
  --group fraud-scoring-service \
  --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true
```

All 10 `initiated` events consumed successfully (sample):
```
Partition:1 | Offset:0 | pay-001 | {"processingState":"digitalpayments.payment.initiated",...,"transaction":{"paymentId":"pay-001",...,"status":"Pending"}}
Partition:3 | Offset:0 | pay-002 | {"processingState":"digitalpayments.payment.initiated",...,"transaction":{"paymentId":"pay-002",...,"status":"Pending"}}
...
Processed a total of 10 messages
```

### Notification Consumer (`payment-notification-service`)

Subscribes to 4 topics via a regex (`--include`), matching the event filtering decision in Section 1.3:

```bash
kubectl exec kafka-0 -- kafka-console-consumer --bootstrap-server localhost:9092 \
  --include 'digitalpayments\.payment\.(unauthorised|invalidated|incomplete|completed)' \
  --group payment-notification-service \
  --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true
```

All 10 terminal outcomes consumed (7 `completed` + 1 `unauthorised` + 1 `invalidated` + 1 `incomplete` — matching the 10 payment journeys exactly, sample):
```
Partition:1 | Offset:0 | pay-001 | {"processingState":"digitalpayments.payment.unauthorised",...,"transaction":{"paymentId":"pay-001",...,"status":"Failed","fraudScore":"High"}}
Partition:0 | Offset:0 | pay-010 | {"processingState":"digitalpayments.payment.completed",...,"transaction":{"paymentId":"pay-010",...,"status":"Completed","fraudScore":"Medium"}}
Partition:2 | Offset:0 | pay-003 | {"processingState":"digitalpayments.payment.invalidated",...,"transaction":{"paymentId":"pay-003",...,"status":"Failed","fraudScore":"Medium"}}
Partition:0 | Offset:0 | pay-004 | {"processingState":"digitalpayments.payment.incomplete",...,"transaction":{"paymentId":"pay-004",...,"status":"Failed","fraudScore":"Medium"}}
...
Processed a total of 10 messages
```

The `--include` regex correctly excluded `digitalpayments.payment.authorised`, `.validated`, `digitalpayments.fraud.scored`, and the unrelated scratch topics — only the 4 designed topics were consumed.

### Reconciliation/Audit Consumer (`digitalpayments-payment-retention-service`)

Subscribes to all 9 payment + fraud + notification topics via regex (updated from 8 to 9 once the notification topic was designed):

```bash
kubectl exec kafka-0 -- kafka-console-consumer --bootstrap-server localhost:9092 \
  --include 'digitalpayments\.(payment\.(initiated|authorised|unauthorised|validated|invalidated|completed|incomplete)|fraud\.scored|notification\.processed)' \
  --group digitalpayments-payment-retention-service \
  --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true
```

**Run 1 (8 topics, before the notification topic existed): 47 messages consumed — exactly matching the 47 total messages produced** (10 initiated + 10 fraud.scored + 9 authorised + 1 unauthorised + 8 validated + 1 invalidated + 7 completed + 1 incomplete). The regex also correctly excluded `digitalpayments.payments.lifecycle` and `my-topic` (the unrelated scratch topics identified in Section 2), even though the former's name also starts with `digitalpayments.`.

**Run 2 (regex updated to 9 topics, after `notification.processed` was produced): only 10 new messages consumed** — not 57. This is correct, not a bug: the group already had committed offsets at end-of-log for all 8 original topics from Run 1, so it consumed only the unread messages on the newly-added `notification.processed` topic, with zero duplication. `kafka-consumer-groups --describe` confirms **zero lag across all 9 topics / all partitions** after this run (Section 5).

---

## 5. Verification

### Consumer Group Status & Lag

```bash
kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <name>
```

All three consumer groups show **zero lag on every partition** they're assigned (`CURRENT-OFFSET` == `LOG-END-OFFSET` everywhere) — each group has fully caught up to everything produced:

```
GROUP                  TOPIC                              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
fraud-scoring-service  digitalpayments.payment.initiated  0          1               1               0
fraud-scoring-service  digitalpayments.payment.initiated  1          1               1               0
...(all 10 partitions, all LAG=0)

payment-notification-service  digitalpayments.payment.completed     0  1  1  0
payment-notification-service  digitalpayments.payment.unauthorised  1  1  1  0
...(all partitions across the 4 subscribed topics, all LAG=0)

digitalpayments-payment-retention-service  digitalpayments.fraud.scored             0  1  1  0
digitalpayments-payment-retention-service  digitalpayments.notification.processed   0  1  1  0
digitalpayments-payment-retention-service  digitalpayments.payment.initiated        0  1  1  0
...(all partitions across all 9 subscribed topics, all LAG=0 — confirmed after notification.processed was added, Section 4)
```

Note: `kafka-consumer-groups --describe` reports "has no active members" for each group alongside this table — expected, since the `kafka-console-consumer` process for each group had already exited (hit its `--timeout-ms` after draining all available messages) by the time this was run. The offset/lag data itself is still accurate; it reflects the group's committed position, not whether a consumer is currently connected.

### Event Ordering Verification

Kafka guarantees ordering only *within* a single topic-partition — it does not guarantee any ordering across the (now 9) separate topics one payment's lifecycle is spread across (see Section 1.1). This was verified directly using `pay-001`'s events (an `auth_fail` journey: `initiated` → `fraud.scored` → `unauthorised`), pulled from the reconciliation consumer's output:

```
Partition:1 | Offset:0 | pay-001 | processingState=digitalpayments.payment.initiated    createdDateTime=2026-09-27T18:39:16.407Z
Partition:1 | Offset:0 | pay-001 | processingState=digitalpayments.fraud.scored          createdDateTime=2026-09-27T18:39:41.104Z  previousSource=digitalpayments.payment.initiated
Partition:1 | Offset:0 | pay-001 | processingState=digitalpayments.payment.unauthorised   createdDateTime=2026-09-27T18:40:15.948Z  previousSource=digitalpayments.fraud.scored
```

All three events landed on **`Partition:1, Offset:0`** — identical partition and offset — but in **three different topics**, which proves partition/offset alone cannot be used to reconstruct order across topics (they're not comparable across different topics). Sorting instead by `createdDateTime` gives the correct causal order (`18:39:16` → `18:39:41` → `18:40:15`), and that order exactly matches the `previousSource` chain (`fraud.scored`'s previous source is `initiated`; `unauthorised`'s previous source is `fraud.scored`). This confirms the ordering/reconstruction strategy designed in Section 1.1 (`paymentId` grouping + `createdDateTime` ordering + `previousSource` chain verification) works as intended.

Within a single topic, ordering-by-key was also confirmed: `pay-001` consistently lands on partition 1 in every 10-partition topic it passes through (`initiated`, `fraud.scored`) — matching the partition key strategy in Section 1.2 (same key → same partition number, within topics of the same partition count).

### Topic Status

All 8 topics confirmed present and correctly configured via `kafka-topics --describe` (Section 2 for the 3 newly-created failure-path topics; the other 5 pre-existed with matching configuration, per Section 2's findings).

---

## 6. Trade-offs & Justifications

A rollup of the "why X over Y" decisions made throughout — full reasoning for each lives in Section 1.

| Decision | Chose | Over | Why |
|---|---|---|---|
| Topic structure | One topic per event type (8 topics) | One combined "payment events" topic | Lets consumers subscribe only to the event types they need (e.g. fraud only needs `initiated`). Cost: no cross-topic ordering guarantee — mitigated by `paymentId` + `createdDateTime` + `previousSource` (Section 1.1, verified in Section 5). |
| Partitions | Generous upfront (10 happy-path/fraud, 5 failure-path) | Starting minimal and growing later | Partition key is `paymentId`; increasing partition count later breaks "same payment → same partition" for future events. Provisioning once avoids that disruption, at the cost of some idle capacity today. |
| Replication factor | 3 (uniform, all topics) | Differentiating by topic criticality | Cluster ceiling is 3 brokers anyway; uniform config is simpler to operate for a POC, at the cost of applying "zero data loss" treatment to topics (e.g. notification) that weren't explicitly named in that requirement. |
| Retention | Kafka holds 7 days + 100GB/partition safety net; a downstream AWS store holds the real 7-year compliance copy | Holding 7 years directly in Kafka | Kafka isn't designed as long-term storage at this throughput; cost/complexity of 7 years on-broker would be prohibitive. Trade-off: correctness depends on the reconciliation consumer never falling behind. |
| Cleanup policy | `delete` | `compact` | These are immutable audit facts, not overwritable state — compaction's "only the latest value per key matters" model would silently discard duplicate/retried events that should stay visible for audit. |
| Compression | `lz4` | `gzip`/`zstd` (better ratio) | Fraud's <50ms SLA makes compress/decompress CPU cost matter more than storage/bandwidth savings on these small JSON payloads. |
| Durability (`acks`) | `acks=all` | `acks=1` (faster) | Only `acks=all` actually uses `min.insync.replicas=2` — `acks=1` acks after the leader alone, making that setting meaningless. Slower, but required by the "zero data loss" language in project.md line 16. |
| Idempotency | Producer-level `enable.idempotence=true` only | Also building exactly-once consumers | Solves producer-retry duplicates at the broker. Consumer-side duplicate protection is deliberately pushed to at-least-once + application-level dedup instead of Kafka transactions, to avoid transactional coordination latency on SLA-sensitive consumers (fraud especially). |
| Serialization | JSON | Avro/Protobuf | Readability/debuggability for a POC (raw `kafka-console-consumer` output, no schema registry) outweighs the compactness and schema-evolution benefits of a binary format. |
| PII (`accountNumber`) | Kept raw | Tokenized/masked | Explicit POC-scope decision using mock data only — would need tokenization before handling real client data. |
| Amount field | Decimal (2 d.p.) | Integer minor-units (cents) | Reversed mid-design after initially choosing integer-cents specifically to avoid float rounding risk (Section 1.1) — final call accepts that risk for simplicity; worth revisiting if this ever handled real money. |
| Consumer processing guarantee | At-least-once, all 3 groups | Exactly-once (Kafka transactions) | Transactional exactly-once adds coordination latency that conflicts with the tightest SLA (fraud, <50ms) — same reasoning applied consistently to notification and reconciliation for architectural consistency, with dedup handled at the appropriate layer per consumer (broker-level idempotence, notification-send dedup, storage-layer upsert key). |

---

## 7. Issues Encountered

| Issue | How it was found | Resolution |
|---|---|---|
| `retention.bytes=1GB` would have emptied every happy-path partition in ~3 hours, not 7 days | Walked through the actual math: `retention.bytes` is per-partition in Kafka, and at ~100 msg/sec/partition even a rough 1KB/message estimate fills 1GB in minutes/hours, not a week | Recalculated the real 7-day requirement (~60GB/partition) and set `retention.bytes=100GB/partition` as a safety net *above* that baseline, not below it (Section 1.1 "Retention") |
| `acks=1` was initially chosen for speed, but silently made `min.insync.replicas=2` meaningless | Traced through what `acks=1` actually waits for (leader write only, no follower confirmation) | Switched to `acks=all`, accepting the latency cost, to actually deliver the "zero data loss" requirement (Section 1.2 "Durability") |
| `delivery.timeout.ms=10ms` couldn't fit the requested 3 retries at 10ms backoff (backoff alone needs ≥20ms) | Walked through the arithmetic of attempts + backoff vs. the configured ceiling | Raised to `delivery.timeout.ms=50ms`, leaving room for 3 retries at 10ms backoff plus actual attempt time within the overall <150ms payment SLA (Section 1.2 "Error Handling") |
| Schema draft had `status` and `processingState` disagreeing (message going to `authorised` still showed `status: "Initiated"`), a missing comma, and `amount` flip-flopped between decimal and integer-cents representation across several iterations | Caught during iterative review of the example JSON payload | Settled on `status` as a 3-value coarse business outcome (`Pending`/`Completed`/`Failed`) distinct from `processingState`'s fine-grained pipeline stage, fixed the JSON syntax, and made a final, deliberate call to keep `amount` as decimal (Section 1.1 "Message Schema") |
| The provided Kafka cluster wasn't empty — it already had 4 topics matching this design almost exactly, one topic with a naming typo (`digitalpayment.payment.initiated`, missing the "s"), and 2 unrelated scratch topics | Ran `kafka-topics --list`/`--describe` before creating anything, rather than assuming a clean slate | Left all pre-existing topics untouched (including the `retention.bytes` gap on the 4 correctly-named ones), the typo'd topic was deleted/recreated by the user directly, and the 2 scratch topics were left alone (Section 2) |
| First attempt to verify `fraud.scored` via `kafka-console-consumer --timeout-ms 5000` returned zero messages despite 10 having been sent | A brand-new consumer group's initial join/partition-assignment can take longer than 5 seconds, especially routed through `kubectl exec` | Increased `--timeout-ms` to 10000-20000 for verification reads; a long-running real consumer wouldn't hit this at all since it only affects the CLI tool's one-shot timeout, not steady-state consumption |
| The original command templates in `payment.md` used `--property` to set producer/consumer client configs like `acks`, `retries`, `enable.idempotence` | Checked `kafka-console-producer --help` / `kafka-console-consumer --help` directly rather than assuming the flag worked | `--property` only feeds the message reader/formatter (e.g. `parse.key`, `print.partition`) — actual client configs need `--producer-property`/`--consumer-property`. Fixed across `payment.md`, `fraud.md`, `notification.md` |
| Consumer scalability numbers were initially miscalculated twice: the fraud consumer's "8 instances" was justified by topic count (unrelated to partition count), and the notification consumer's "10 instances" was based on its largest single subscribed topic rather than the sum across all topics it subscribes to | Walked through the actual partition math for each multi-topic subscription | Corrected to: fraud consumer sized off `initiated`'s 10 partitions; notification consumer's real ceiling is 25 (5+5+5+10 across its 4 topics), landing on 20 instances deliberately below that; reconciliation consumer's ceiling is 65 across its 8 topics, also landing on 20 (Section 1.3) |
| The fraud producer's retry/timeout values were initially going to be copied from the payment producer (`delivery.timeout.ms=50ms`), which was sized against the payment lifecycle's 150ms budget, not fraud's much tighter 50ms budget | Noticed while filling in `fraud.md` that reusing the payment producer's numbers would consume the *entire* fraud SLA on the produce step alone | Proposed a separate, tighter set of values for the fraud producer specifically (`fraud.md`) — flagged explicitly as a judgment call needing review, not a value that was actually agreed on in the design conversation |
