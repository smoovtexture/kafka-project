#!/usr/bin/env python3
"""
Generates mock payment lifecycle events and publishes them to Kafka via
`kubectl exec <pod> -- kafka-console-producer`.

Usage (run once per pipeline stage, in order):

    python generate_payment_events.py --topic digitalpayments.payment.initiated    --count 10
    python generate_payment_events.py --topic digitalpayments.fraud.scored          --count 10
    python generate_payment_events.py --topic digitalpayments.payment.authorised    --count 9
    python generate_payment_events.py --topic digitalpayments.payment.unauthorised  --count 1
    python generate_payment_events.py --topic digitalpayments.payment.validated     --count 8
    python generate_payment_events.py --topic digitalpayments.payment.invalidated   --count 1
    python generate_payment_events.py --topic digitalpayments.payment.completed     --count 7
    python generate_payment_events.py --topic digitalpayments.payment.incomplete    --count 1

The `--topic digitalpayments.payment.initiated` call creates brand-new mock
payments (assigning each one an outcome: success / auth_fail / validation_fail /
completion_fail). Every later call advances the *existing* payments that are
eligible for that topic (correct outcome bucket + correct current stage),
reusing their paymentId, accounts, amount and fraud score so a payment's
events stay correlated across topics. State is tracked in a local JSON file
(--state-file) between runs.
"""
import argparse
import json
import random
import string
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_STATE_FILE = Path(__file__).parent / "payment_state.json"

# previousSource for each topic, i.e. which topic precedes it in the pipeline
PREVIOUS_SOURCE = {
    "digitalpayments.payment.initiated": None,
    "digitalpayments.fraud.scored": "digitalpayments.payment.initiated",
    "digitalpayments.payment.authorised": "digitalpayments.fraud.scored",
    "digitalpayments.payment.unauthorised": "digitalpayments.fraud.scored",
    "digitalpayments.payment.validated": "digitalpayments.payment.authorised",
    "digitalpayments.payment.invalidated": "digitalpayments.payment.authorised",
    "digitalpayments.payment.completed": "digitalpayments.payment.validated",
    "digitalpayments.payment.incomplete": "digitalpayments.payment.validated",
}

# which "stage" a payment must currently be at to be eligible for this topic,
# and which outcome bucket is allowed to advance to it
ELIGIBILITY = {
    "digitalpayments.fraud.scored": {"from_stage": "digitalpayments.payment.initiated", "outcomes": None},
    "digitalpayments.payment.authorised": {"from_stage": "digitalpayments.fraud.scored", "outcomes": {"success", "validation_fail", "completion_fail"}},
    "digitalpayments.payment.unauthorised": {"from_stage": "digitalpayments.fraud.scored", "outcomes": {"auth_fail"}},
    "digitalpayments.payment.validated": {"from_stage": "digitalpayments.payment.authorised", "outcomes": {"success", "completion_fail"}},
    "digitalpayments.payment.invalidated": {"from_stage": "digitalpayments.payment.authorised", "outcomes": {"validation_fail"}},
    "digitalpayments.payment.completed": {"from_stage": "digitalpayments.payment.validated", "outcomes": {"success"}},
    "digitalpayments.payment.incomplete": {"from_stage": "digitalpayments.payment.validated", "outcomes": {"completion_fail"}},
}

# terminal/business status shown on the event, per topic
STATUS = {
    "digitalpayments.payment.initiated": "Pending",
    "digitalpayments.fraud.scored": "Pending",
    "digitalpayments.payment.authorised": "Pending",
    "digitalpayments.payment.unauthorised": "Failed",
    "digitalpayments.payment.validated": "Pending",
    "digitalpayments.payment.invalidated": "Failed",
    "digitalpayments.payment.completed": "Completed",
    "digitalpayments.payment.incomplete": "Failed",
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_account_number():
    return "".join(random.choices(string.digits, k=10))


def load_state(state_file):
    if state_file.exists():
        return json.loads(state_file.read_text())
    return []


def save_state(state_file, payments):
    state_file.write_text(json.dumps(payments, indent=2))


def next_payment_id(payments):
    return f"pay-{len(payments) + 1:03d}"


def assign_outcomes(count):
    """70% success / 10% auth_fail / 10% validation_fail / 10% completion_fail (rounded)."""
    n_auth_fail = count // 10
    n_val_fail = count // 10
    n_comp_fail = count // 10
    n_success = count - n_auth_fail - n_val_fail - n_comp_fail
    outcomes = (
        ["success"] * n_success
        + ["auth_fail"] * n_auth_fail
        + ["validation_fail"] * n_val_fail
        + ["completion_fail"] * n_comp_fail
    )
    random.shuffle(outcomes)
    return outcomes


def create_new_payments(payments, count):
    created = []
    for outcome in assign_outcomes(count):
        record = {
            "paymentId": next_payment_id(payments),
            "payerAccountNumber": random_account_number(),
            "payerAccountType": "SVGS",
            "payeeAccountNumber": random_account_number(),
            "payeeAccountType": "SVGS",
            "amount": round(random.uniform(10, 5000), 2),
            "outcome": outcome,
            "fraudScore": None,
            "stage": "digitalpayments.payment.initiated",
        }
        payments.append(record)
        created.append(record)
    return created


def pick_eligible(payments, topic, count):
    rule = ELIGIBILITY[topic]
    eligible = [
        p for p in payments
        if p["stage"] == rule["from_stage"]
        and (rule["outcomes"] is None or p["outcome"] in rule["outcomes"])
    ]
    if len(eligible) < count:
        print(f"warning: requested {count} but only {len(eligible)} payments are eligible for {topic}; using {len(eligible)}", file=sys.stderr)
    return eligible[:count]


def build_message(record, topic):
    if topic == "digitalpayments.fraud.scored":
        record["fraudScore"] = "High" if record["outcome"] == "auth_fail" else random.choice(["Low", "Medium"])

    transaction = {
        "paymentId": record["paymentId"],
        "payerAccountNumber": record["payerAccountNumber"],
        "payerAccountType": record["payerAccountType"],
        "amount": record["amount"],
        "payeeAccountNumber": record["payeeAccountNumber"],
        "payeeAccountType": record["payeeAccountType"],
        "status": STATUS[topic],
    }
    if record["fraudScore"] is not None:
        transaction["fraudScore"] = record["fraudScore"]

    payload = {
        "processingState": topic,
        "retryCount": 0,
    }
    previous_source = PREVIOUS_SOURCE[topic]
    if previous_source is not None:
        payload["previousSource"] = previous_source
    payload["createdDateTime"] = now_iso()
    payload["transaction"] = transaction

    record["stage"] = topic
    return record["paymentId"], json.dumps(payload, separators=(",", ":"))


def send_to_kafka(topic, lines, pod, bootstrap_server):
    cmd = [
        "kubectl", "exec", "-i", pod, "--",
        "kafka-console-producer",
        "--bootstrap-server", bootstrap_server,
        "--topic", topic,
        "--property", "parse.key=true",
        "--property", "key.separator=:",
    ]
    stdin_data = "\n".join(lines) + "\n"
    result = subprocess.run(cmd, input=stdin_data, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(f"kafka-console-producer failed with exit code {result.returncode}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topic", required=True, choices=sorted(STATUS.keys()), help="Target topic to publish to")
    parser.add_argument("--count", required=True, type=int, help="Number of messages to generate for this call")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE, help="Path to the local JSON file tracking payment journeys across calls")
    parser.add_argument("--pod", default="kafka-0", help="Kafka broker pod to exec into (default: kafka-0)")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Bootstrap server address as seen from inside the pod")
    parser.add_argument("--dry-run", action="store_true", help="Print generated messages without sending them to Kafka")
    args = parser.parse_args()

    payments = load_state(args.state_file)

    if args.topic == "digitalpayments.payment.initiated":
        targets = create_new_payments(payments, args.count)
    else:
        targets = pick_eligible(payments, args.topic, args.count)

    if not targets:
        print("Nothing to send.", file=sys.stderr)
        return

    lines = []
    for record in targets:
        key, value = build_message(record, args.topic)
        lines.append(f"{key}:{value}")
        print(f"{record['paymentId']} [{record['outcome']}] -> {args.topic}")

    if args.dry_run:
        print("\n--dry-run: not sending--\n")
        print("\n".join(lines))
    else:
        send_to_kafka(args.topic, lines, args.pod, args.bootstrap_server)
        print(f"Sent {len(lines)} message(s) to {args.topic}")

    save_state(args.state_file, payments)


if __name__ == "__main__":
    main()
