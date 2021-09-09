#!/usr/bin/python3

import argparse
import csv
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

from dateutil.parser import parse as dateparse

import heliumapi as api

log = logging.getLogger(__file__)
# When debugging this code, keep request/urllib3 from spamming the logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


def _daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _earnings_daily(address, start, stop):
    """
    Get daily subtotal for all earnings [start, end) for the given hotspot or
    validator address.

    Returns
    A dict of {"iso8601 date string" : daily subtotal in bones}, with one
    key for each day that had earnings.

    Will return an empty dict if the address is not found or no earnings
    were found.
    """
    log.debug(f"Getting data for {address} from {start} to {stop}")
    ret = defaultdict(lambda: {"hnt": 0.0, "usd": 0.0}, key=str)
    rewards = api.earnings(address, start, stop)

    # Convert 'timestamp' from an ISO8601 string to a datetime then "truncate"
    # to a datetime.date(), using this as the key, filling in HNT and daily price
    # in the value dict
    for r in rewards:
        day = dateparse(r["timestamp"]).date()
        bones = r["sum"]
        if bones > 0:
            hnt = bones / api.BONES_PER_HNT
            price = api.oracle_price_for_day(day) / api.BONES_PER_HNT
            ret[day]["hnt"] = hnt
            ret[day]["price"] = price

    del ret["key"]
    return ret


def _arg_valid_date(s):
    try:
        today = datetime.now().astimezone().date()
        day = date.fromisoformat(s)
        return day if day < today else today
    except Exception as e:
        msg = f"Not a valid iso8601 date: '{s}'. Must be in YYYY-MM-DD format. "
        "an no later than today"
        log.error(e)
        raise argparse.ArgumentTypeError(msg)


def _write_csv(data):
    writer = csv.writer(sys.stdout, dialect="unix")
    writer.writerow(["date", "hnt", "price"])
    for date, v in sorted(data.items(), reverse=True):
        writer.writerow((date, v["hnt"], v["price"]))


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
        help="Begining of time range as iso8601 date string in UTC. Defaults to yesterday.",
        default=(today - timedelta(days=1)).date().isoformat(),
        type=_arg_valid_date,
    )
    parser.add_argument(
        "--stop",
        help="End of time range as iso8601 date string in UTC. "
        "Can be no later than today. Defaults to today.",
        required=False,
        default=today.date().isoformat(),
        type=_arg_valid_date,
    )

    args = parser.parse_args()
    ret = _earnings_daily(args.address, args.start, args.stop)
    _write_csv(ret)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
