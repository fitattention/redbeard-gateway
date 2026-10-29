"""Operator commands (run on the server), and a manual fallback to credit a late or short crypto payment.

  python admin.py new starter          a new key with 30 days paid (also power, max). The key is printed ONCE.
  python admin.py new payg 20          a new pay-as-you-go key holding $20
  python admin.py extend KEY           one more payment: 30 more days on a plan key
  python admin.py topup KEY 20         add $20 of credit to a key (on a plan key: extra usage)
  python admin.py revoke KEY           refuse the key from now on
  python admin.py show KEY             plan, paid until, usage
  python admin.py order REF            a checkout order: what it was for, credited or not (support)
  python admin.py credit REF           give that order's key what was paid for: a late or short payment (support)

KEY is the customer's key (rb_...) or its 64-character fingerprint. Keys file: REDBEARD_DB, or keys.db here.
"""
import sys
import time
from datetime import datetime, timezone

import meter
import store


def when(ts):
    return "never" if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def dollars(text):
    amount = float(text)
    if not 0 < amount <= 10_000:
        raise ValueError("the amount must be above 0 and at most 10000 dollars")
    return round(amount * 1_000_000)


def find(conn, key):
    key_hash = store.hash_key(key) if key.startswith("rb_") else key.lower()
    k = store.get(conn, key_hash)
    if k is None:
        raise ValueError("no such key")
    return k


def describe(k):
    if k.plan == "payg":
        return f"plan payg, balance ${k.balance_micros / 1_000_000:.2f}, revoked {bool(k.revoked)}"
    return (f"plan {k.plan}, paid until {when(k.paid_until)}, used {k.fh_used:,} in this 5-hour window, "
            f"{k.wk_used:,} this week, revoked {bool(k.revoked)}"
            + (f", extra usage ${k.balance_micros / 1_000_000:.2f}" if k.balance_cap or k.balance_micros else ""))


def main(args, conn):
    now = int(time.time())
    if len(args) >= 2 and args[0] == "new":
        plan = args[1]
        if plan not in store.PLAN_IDS:
            raise ValueError(f"plan must be one of {', '.join(store.PLAN_IDS)}")
        raw = store.new_raw_key()
        k = store.Key(key_hash=store.hash_key(raw), plan=plan)
        if plan == "payg":
            k.balance_micros = k.balance_cap = dollars(args[2] if len(args) > 2 else "")
        else:
            meter.extend(k, now)
        meter.refresh(k, now)
        store.put(conn, k)
        return f"{raw}\n(shown once: give it to the customer now; only its fingerprint is kept)\n{describe(k)}"
    if len(args) >= 2 and args[0] in ("extend", "topup", "revoke", "show"):
        k = find(conn, args[1])
        if args[0] == "extend":
            if k.plan == "payg":
                raise ValueError("pay-as-you-go keys are topped up, not extended")
            meter.extend(k, now)
        elif args[0] == "topup":
            k.balance_micros += dollars(args[2] if len(args) > 2 else "")
            k.balance_cap = k.balance_micros       # a top-up fills the tank: the bar reads full again
        elif args[0] == "revoke":
            k.revoked = 1
        store.put(conn, k)
        return describe(k)
    if len(args) >= 2 and args[0] in ("order", "credit"):
        import checkout  # support: the customer sends the order reference and the transaction ID, never the key
        o = checkout.get_order(conn, args[1])
        if not o:
            raise ValueError("no such order (finished ones are cleared after a day, unfinished after a week)")
        if args[0] == "credit":
            if o["done"]:
                raise ValueError("this order was already credited")
            checkout.credit(conn, args[1])
            o = checkout.get_order(conn, args[1])
        k = store.get(conn, o["key_hash"])
        what = f"{o['plan']} plan, 30 days" if o["kind"] == "plan" else "pay-as-you-go top-up"
        return (f"order {o['order_id']}: {what}, ${o['cents'] / 100:.2f}, made {when(o['created'])}, "
                f"{'credited' if o['done'] else 'NOT credited'}; Bitcart invoice {o['invoice_id']}\n"
                f"key: {describe(k) if k else 'none yet (it is created when the order is credited)'}")
    raise ValueError("unknown command")


if __name__ == "__main__":
    try:
        print(main(sys.argv[1:], store.connect(store.DEFAULT_DB)))
    except ValueError as e:
        sys.exit(f"{e}\n\n{__doc__}")
