#!/usr/bin/python3

import argparse
import csv
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from dateutil.parser import parse as dateparse

import heliumapi as api

log = logging.getLogger(__file__)
# When debugging this code, keep request/urllib3 from spamming the logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


def hotspot_earnings_daily(address, start, stop):
    """
    Get daily subtotal for all earnings [start, end) for the given hotspot
    address.

    Returns
    A dict of {"iso8601 date string" : daily subtotal in bones}, with one
    key for each day that had earnings.

    Will return an empty dict if the address is not found or no earnings
    were found.
    """
    ret = defaultdict(lambda: {"hnt": 0.0, "usd": 0.0}, key=str)
    rewards = api.hotspot_earnings(address, start, stop)

    # Convert 'timestamp' from an ISO8601 string to a datetime then "truncate"
    # to a datetime.date(), using this as a key and the subtotalling daily
    # rewards and their USD equivalent.
    for r in rewards:
        day = dateparse(r["timestamp"]).date().isoformat()
        hnt = r["amount"] / api.BONES_PER_HNT
        price = api.oracle_price_at_block(r["block"]) / api.BONES_PER_HNT
        ret[day]["hnt"] += hnt
        ret[day]["usd"] += price * hnt
        log.debug(f"{day} -- {hnt} {price} {price * hnt}  -- totals: {ret[day]}")
    del ret["key"]
    return ret


def arg_valid_date(s):
    try:
        dt = dateparse(s)
        if dt.tzinfo is None:
            dt = dt.astimezone()

        return dt.isoformat()
    except ValueError:
        msg = "Not a valid iso8601 datetime: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def write_csv(data):
    writer = csv.writer(sys.stdout, dialect="unix")
    writer.writerow(["date", "hnt", "usd"])
    for date, v in sorted(data.items()):
        writer.writerow((date, v["hnt"], v["usd"]))


def main():
    today = datetime.now().astimezone()
    parser = argparse.ArgumentParser(
        description="Get a daily rollup of earnings for a hotspot between a "
        "range of timestamps. Results are in the "
        "range [start, stop)."
    )
    parser.add_argument("address", help="A helium hotspot address", type=str)

    parser.add_argument(
        "--start",
        help="Begining of time range as iso8601 string. Defaults to yesterday.",
        default=(today - timedelta(days=1)).isoformat(),
        type=arg_valid_date,
    )
    parser.add_argument(
        "--stop",
        help="End of time range as iso8601 string. Defaults to today.",
        required=False,
        default=today.isoformat(),
        type=arg_valid_date,
    )

    args = parser.parse_args()
    ret = hotspot_earnings_daily(args.address, args.start, args.stop)
    write_csv(ret)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
