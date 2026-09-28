# Notification producer, consumer, topic/s and event/s

Full design rationale lives in [payments-event-setup.md](payments-event-setup.md) Section 1. This file is the quick-reference command sheet for the notification domain.

## Notification Consumer (`payment-notification-service`) — designed

Subscribes to the 4 outcome topics that decide what to notify the customer about: all 3 failure-path topics (a "declined" notification) plus `completed` only, not `validated` (an "approved" notification, sent once at the final outcome — see payments-event-setup.md Section 1.3 for why `validated` was deliberately excluded). Lag SLA: **<2s** (project.md line 15).

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --include 'digitalpayments\.payment\.(unauthorised|invalidated|incomplete|completed)' \
  --group payment-notification-service \
  --property parse.key=true \
  --property print.partition=true --property print.offset=true \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property max.poll.records=500 \
  --consumer-property fetch.min.bytes=1024 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

- `enable.auto.commit=false`: manual commit after the notification is actually sent (Section 1.3) — at-least-once, with duplicate-notification prevention pushed into the send logic itself (dedup on `paymentId` + `payerAccountNumber` + outcome), since a duplicate here is customer-visible, unlike a duplicate fraud score.
- `fetch.min.bytes=1024` / `fetch.max.wait.ms=500`: can batch more aggressively than the fraud consumer, given the much looser 2s SLA — larger, less frequent fetches are more efficient.
- Scalability: up to **20 parallel instances** (this group subscribes to 25 partitions total across its 4 topics — 5+5+5+10 — see Section 1.3 for why the ceiling is the *sum* across all subscribed topics, not just the largest one).
- Sample output and verified zero-lag status: payments-event-setup.md Section 4/5.

## Notification Topic: `digitalpayments.notification.processed`

Deliberately kept as **one topic with an `outcome`-style field**, not split by outcome like the payment domain — this is a flat terminal record (no further lifecycle after it), not a multi-step process, so there was no filtering-by-event-type reason to split it. It captures **two independent dimensions**: whether the *payment* was approved or failed (`paymentOutcome`), and whether the *notification itself* was successfully delivered (`notificationOutcome`) — a payment can complete fine while its notification still fails to reach the customer (e.g. bad phone number, downed SMS gateway), which is exactly why this topic is not redundant with `payment.completed`/`.incomplete`.

10 partitions, RF=3, `min.insync.replicas=2`, `cleanup.policy=delete`, 7-day + 100GB/partition retention, `lz4` compression (producer-side) — consistent with every other topic in this design, since nothing about this topic's volume (one record per payment, same order of magnitude as `initiated`) or criticality argued for differing from precedent.

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.notification.processed \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config retention.bytes=107374182400
```

**Schema example:**
```json
{
  "processingState": "digitalpayments.notification.processed",
  "retryCount": 0,
  "previousSource": "digitalpayments.payment.completed",
  "createdDateTime": "2026-09-27T00:00:00.000Z",
  "paymentOutcome": "Approved",
  "notificationOutcome": true,
  "transaction": {
    "paymentId": "pay-01",
    "status": "Completed",
    "fraudScore": "Low"
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `previousSource` | string (topic name) | Whichever payment topic actually triggered this notification: `completed`, `incomplete`, `unauthorised`, or `invalidated`. |
| `paymentOutcome` | enum: `Approved` \| `Failed` | The payment outcome being communicated to the customer — matches project.md's "payment approved or failed" language. |
| `notificationOutcome` | boolean | Whether the notification was actually delivered — independent of `paymentOutcome`. Deliberately a native JSON boolean with a self-explanatory name, not a bare `0`/`1`, to stay consistent with the readability rationale for choosing JSON in the first place (Section 1.2). |
| `transaction.paymentId` | string | Correlation key, also the Kafka message key (same as every other topic). |
| `transaction.status` / `transaction.fraudScore` | as defined in the payment schema | Kept for context; account numbers/amount/payee details deliberately dropped — not relevant to a notification-delivery record. |

**Consumer subscription update:** the reconciliation/audit consumer (`digitalpayments-payment-retention-service`) now subscribes to this topic too, for a complete audit trail — bringing its total to **9 topics / 75 partitions** (previously 8 topics / 65 partitions).

## Notification Producer

All decisions kept consistent with the payment producer's precedent (Section 1.2) — nothing about this producer's looser downstream SLA (<2s notification, <1min reconciliation, both looser than the payment producer's own <150ms) argued for diverging:

- **Partition key:** `paymentId` — same correlation/reconstruction strategy as every other topic.
- **Durability:** `acks=all`.
- **Idempotency:** `enable.idempotence=true`.
- **Serialization:** JSON, same readability rationale as the rest of the system.
- **Error handling:** same `retries=3`, `retry.backoff.ms=10`, `delivery.timeout.ms=50` as the payment producer (no tightening needed here the way it was for the fraud producer, since this producer isn't gating anything with a tight SLA).
- **Production scale:** fires once per payment journey — 10 total, matching the 10 mock payments already produced.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.notification.processed \
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
