#!/usr/bin/env python3
"""
Generates the 10 digitalpayments.notification.processed events, one per
payment, derived from the terminal stage each payment reached in
payment_state.json (produced by generate_payment_events.py).

Usage:
    python generate_notification_events.py [--dry-run]
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

STATE_FILE = Path(__file__).parent / "payment_state.json"
TOPIC = "digitalpayments.notification.processed"

# one deliberate notification-delivery failure, independent of payment outcome,
# to demonstrate the two dimensions are genuinely independent
NOTIFICATION_DELIVERY_FAILURES = {"pay-005"}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_message(record):
    stage = record["stage"]
    payment_outcome = "Approved" if stage == "digitalpayments.payment.completed" else "Failed"
    status = "Completed" if stage == "digitalpayments.payment.completed" else "Failed"
    notification_outcome = record["paymentId"] not in NOTIFICATION_DELIVERY_FAILURES

    payload = {
        "processingState": TOPIC,
        "retryCount": 0,
        "previousSource": stage,
        "createdDateTime": now_iso(),
        "paymentOutcome": payment_outcome,
        "notificationOutcome": notification_outcome,
        "transaction": {
            "paymentId": record["paymentId"],
            "status": status,
            "fraudScore": record["fraudScore"],
        },
    }
    return record["paymentId"], json.dumps(payload, separators=(",", ":"))


def send_to_kafka(lines, pod="kafka-0", bootstrap_server="localhost:9092"):
    cmd = [
        "kubectl", "exec", "-i", pod, "--",
        "kafka-console-producer",
        "--bootstrap-server", bootstrap_server,
        "--topic", TOPIC,
        "--property", "parse.key=true",
        "--property", "key.separator=:",
    ]
    result = subprocess.run(cmd, input="\n".join(lines) + "\n", text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(f"kafka-console-producer failed with exit code {result.returncode}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payments = json.loads(STATE_FILE.read_text())
    lines = []
    for record in payments:
        key, value = build_message(record)
        lines.append(f"{key}:{value}")
        print(f"{key} -> {TOPIC} ({value})")

    if args.dry_run:
        print("\n--dry-run: not sending--")
    else:
        send_to_kafka(lines)
        print(f"Sent {len(lines)} message(s) to {TOPIC}")


if __name__ == "__main__":
    main()
