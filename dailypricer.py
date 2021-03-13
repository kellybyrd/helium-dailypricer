#!/usr/bin/python3

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from dateutil.parser import parse as dateparse

import argparse
import logging
import json
import requests
import csv
import sys

log = logging.getLogger(__file__)
# When debugging this code, keep request/urllib3 from spamming the logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


BONES_PER_HNT = 100000000
API_URL = "https://api.helium.io/v1"

# Cache of retrieved Helium Oracle prices at specific blocks
oracle_cache = {}


def oracle_price_at_block(block):
    """
    Return the Helium API oracle price in bones at a given block
    """
    if block in oracle_cache:
        log.debug(f"Cached price for block {block} is {oracle_cache[block]}")
        return oracle_cache[block]

    url = f"{API_URL}/oracle/prices/{block}"
    ret = paged_api_request(url)["price"]
    oracle_cache[block] = ret
    log.debug(f"Returning price for block {block} is {oracle_cache[block]}")
    return ret


def paged_api_request(url, query_params={}):
    """
    Takes a helium api URL which returns JSON and may have paged results as
    described in the "Cursors" section here: 
    https://docs.helium.com/api/blockchain/introduction

    Merges the "data" keys from all pages
    
    Returns
    List of dicts from merged results.
    
    Note: This assumes all the repsonse json has "data" and optionaly
          "cursor" top level keys. So far has been true
    """
    ret = list()
    cursor = None

    # repeat-until cursor is None
    while True:
        if cursor is not None:
            query_params["cursor"] = cursor

        try:
            resp = requests.get(url, params=query_params)
            resp.raise_for_status()
            json = resp.json()
            data = json.get("data")
            if type(data) is list:
                ret.extend(data)
                log.debug(f"Ret size is now: {len(ret)}")
                cursor = json.get("cursor")
                log.debug(f"New cursor is : {cursor}")
            else:
                # Not a JSON array, so not a paged result
                ret = data
                cursor = None

        except Exception as ex:
            log.error(f"Error: {ex}")
            # This will break us out of the while loop
            cursor = None

        if cursor is None:
            break

    return ret


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
    url = f"{API_URL}/hotspots/{address}/rewards"
    params = dict()
    params["max_time"] = stop
    params["min_time"] = start

    log.info(f"Getting results from: {start} to {stop}")
    rewards = paged_api_request(url, params)

    # Use a Counter to sum up rewards by days.
    # Convert 'timestamp' from an ISO8601 string to a datetime then "truncate"
    # to a datetime.date(), which causes Counter to group by day.
    #
    # I think this is more readable than the comprehensions way of doing this.
    for r in rewards:
        day = dateparse(r["timestamp"]).date().isoformat()
        hnt = r["amount"] / BONES_PER_HNT
        price = oracle_price_at_block(r["block"]) / BONES_PER_HNT
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
    for date, v in data.items():
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
    # TODO: Output in something more useful.

    print(f"Address: {args.address}")
    write_csv(ret)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
