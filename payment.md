# Payment producer, topic/s and event/s

Full design rationale for everything below lives in [payments-event-setup.md](payments-event-setup.md) Section 1. This file is the quick-reference command sheet for the payment domain specifically.

## Topics

7 topics, one per lifecycle event type — see payments-event-setup.md Section 1.1 for why event-per-topic (not one combined topic) was chosen, and why partition counts differ (10 for happy-path, 5 for failure-path).

### Happy-path topics (10 partitions) — already exist in the cluster

`digitalpayments.payment.initiated`, `digitalpayments.payment.authorised`, `digitalpayments.payment.validated`, `digitalpayments.payment.completed` were already present before this design pass and match the intended config, **except** `retention.bytes` was never set on them (left as-is — see payments-event-setup.md Section 2 for why). Command shown is what created them (already run, not something to re-run):

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.payment.initiated \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config max.message.bytes=12000
```
(Repeat with `--topic digitalpayments.payment.authorised`, `digitalpayments.payment.validated`, `digitalpayments.payment.completed` for the other three — same config.)

**Note:** `compression.type` is deliberately *not* set here — compression is configured on the **producer** (see below), not the topic; the topic-level setting was intentionally left at its default. If you want to close the `retention.bytes` gap on these 4 topics later, use `kafka-configs --alter` (see payments-event-setup.md Section 2) rather than recreating them.

### Failure-path topics (5 partitions) — created during this design pass, full config including `retention.bytes`

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.payment.unauthorised \
  --partitions 5 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config retention.bytes=107374182400
```
(Repeat with `--topic digitalpayments.payment.invalidated` and `digitalpayments.payment.incomplete` — same config, 5 partitions, all already created — see payments-event-setup.md Section 2.)

## Producer configs

One producer, publishing to whichever of the 7 topics matches the payment's current lifecycle stage (topic argument changes per call; every other setting is identical across all 7 — see payments-event-setup.md Section 1.2 for the reasoning behind each value):

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.initiated \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property retries=3 \
  --producer-property retry.backoff.ms=10 \
  --producer-property delivery.timeout.ms=50 \
  --producer-property request.timeout.ms=20 \
  --producer-property linger.ms=0 \
  --producer-property batch.size=16384 \
  --producer-property max.in.flight.requests.per.connection=3 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4
```

Key corrections vs. the earlier draft: `acks=1` → `acks=all` (min.insync.replicas=2 only has effect under acks=all — see payments-event-setup.md Section 1.2 "Durability"), `--property` → `--producer-property` for actual client configs (`--property` only feeds the message reader, e.g. `parse.key`/`key.separator`), `compression.type=LZ4` → lowercase `lz4` (Kafka rejects uppercase), and `delivery.timeout.ms`/`request.timeout.ms`/`linger.ms` reworked to satisfy Kafka's constraint `delivery.timeout.ms >= linger.ms + request.timeout.ms` (50 ≥ 0 + 20 ✓) while still leaving room for the 3 retries at 10ms backoff within the 50ms envelope (see payments-event-setup.md Section 1.2 "Error Handling").

**Actual production for this POC** was done via a wrapper script rather than typing each message by hand — see [scripts/generate_payment_events.py](scripts/generate_payment_events.py) and payments-event-setup.md Section 3.

## Consumer configs (reference template)

Payment topics are read by all three consumer groups (fraud, notification, reconciliation) — see payments-event-setup.md Section 1.3 and Section 4 for the concrete `--group`/`--topic`/`--include` invocation per group. Shared tuning values, filled in per the manual-commit design decision (Section 1.3 — commit only after downstream processing succeeds, never auto-commit):

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic <topic name> \
  --group <consumer group name> \
  --property parse.key=true \
  --property print.partition=true --property print.offset=true \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property max.poll.records=100 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=10 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

Notes:
- `enable.auto.commit=false` + `auto.offset.reset=earliest`: matches the manual-commit design for all three consumer groups; `earliest` ensures no event is ever silently skipped if a group has no prior committed offset (consistent with the audit/zero-data-loss posture).
- `fetch.min.bytes=1` / `fetch.max.wait.ms=10`: tuned for the fraud consumer's <50ms SLA (don't wait to batch — return data as soon as it's available). The notification (`payment-notification-service`) and reconciliation (`digitalpayments-payment-retention-service`) consumer groups can afford larger values here (e.g. `fetch.min.bytes=1024`, `fetch.max.wait.ms=500`+) given their much looser 2s/1min SLAs — worth tuning per group rather than using one value everywhere in a real (non-CLI-console) implementation.
- `partition.assignment.strategy=CooperativeStickyAssignor`: minimizes rebalance disruption, relevant given the notification and reconciliation groups run up to 20 parallel instances each.
- `kafka-console-consumer` itself auto-commits by default regardless of this setting being passed through — see the caveat in payments-event-setup.md Section 4; this only fully applies once a real client application replaces the CLI tool.
