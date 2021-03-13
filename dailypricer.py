#!/usr/bin/python3

from collections import Counter
from datetime import datetime
from dateutil.parser import parse as dateparse

import logging
import json
import requests

log = logging.getLogger(__name__)
# When debugging this code, keep request/urllib3 from spamming the logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


BONES_PER_HNT = 100000000
API_URL = "https://api.helium.io/v1"


def paged_api_request(url, query_params={}):
    """
    Takes a helium api URL which returns JSON and may have paged results as
    described in the "Cursors" section here: 
    https://docs.helium.com/api/blockchain/introduction

    Merges the "data" keys from all pages
    
    Returns
    List of dicts from merged results.
    
    TODO: is this too specific? Maybe the caller should parse the JSON?
    """
    ret = []
    cursor = None

    # "repeat-until cursor is None"
    while True:
        if cursor is not None:
            query_params["cursor"] = cursor

        try:
            resp = requests.get(url, params=query_params)
            resp.raise_for_status()
            tmp = resp.json()
            if "data" in tmp:
                ret.extend(tmp["data"])
                log.debug(f"Ret size is now: {len(ret)}")

            cursor = tmp.get("cursor")
            log.debug(f"New cursor is : {cursor}")

        except Exception as ex:
            log.error(f"Error: {ex}")
            # This will break us out of the while loop
            cursor = None

        if cursor is None:
            log.debug(f"Exiting with ret size: {len(ret)}")
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
    url = f"{API_URL}/hotspots/{address}/rewards"
    params = dict()
    params["max_time"] = stop
    params["min_time"] = start
    rewards = paged_api_request(url, params)

    # Use a Counter to sum up rewards by days.
    # Convert 'timestamp' from an ISO8601 string to a datetime then "truncate"
    # to a datetime.date(), which causes Counter to group by day.
    #
    # I think this is more readable than the comprehensions way of doing this.
    daily = Counter()
    for r in rewards:
        day = dateparse(r["timestamp"]).date().isoformat()
        daily.update({day: r["amount"]})

    return dict(daily)


def main():
    logging.basicConfig(level=logging.INFO)
    # TODO: add argparse to capture these params.
    ret = hotspot_earnings_daily(address="XXXX", start="2021-01-10", stop="2021-03-13")
    # TODO: Output in something more useful.
    print(json.dumps(ret, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
