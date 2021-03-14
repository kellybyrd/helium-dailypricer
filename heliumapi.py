import atexit
import json
import logging
import sqlite3
from datetime import datetime, timedelta

import requests
from dateutil.parser import parse as dateparse

log = logging.getLogger(__name__)

BONES_PER_HNT = 100000000
API_URL = "https://api.helium.io/v1"

_DB = None
_DB_FILE = "cache.sqlite"


def _close_db():
    if _DB is not None:
        _DB.close()


_DB = sqlite3.connect(_DB_FILE)
cur = _DB.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS OraclePrices "
    "(block INTEGER PRIMARY KEY, "
    "price INTEGER DEFAULT 0);"
)

cur.execute(
    "CREATE TABLE IF NOT EXISTS DailyRewards ("
    "hash TEXT PRIMARY KEY, "
    "timestamp TEXT NOT NULL, "
    "address TEXT NOT NULL, "
    "block INTEGER NOT NULL, "
    "amount INTEGER NOT NULL "
    ");"
)

atexit.register(_close_db)
# END MODULE INIT


def _daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _api_request(url, query_params={}):
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


def _db_oracle_fetch(block):
    """
    Lookup the Oracle price from the db

    Returns:
    price in bones or None if it isn't in the db
    """
    cur = _DB.cursor()
    cur.execute("SELECT price FROM OraclePrices WHERE block=:block", {"block": block})
    ret = cur.fetchone()
    if ret is not None:
        ret = ret[0]

    return ret


def _db_oracle_put(block, price):
    """
    Save an Oracle price to the db. Does not yet handle errors like an existing
    record
    """
    cur = _DB.cursor()
    cur.execute(
        "INSERT INTO OraclePrices VALUES (:block, :price)",
        {"block": block, "price": price},
    )
    _DB.commit()


def _db_reward_fetch(address, start, stop):
    """
    TODO

    Returns:
    A list of reward records
    """
    ret = []
    cur = _DB.cursor()
    cur.execute(
        "SELECT hash, timestamp, address, block, amount FROM DailyRewards "
        "WHERE  address=:addr AND "
        "timestamp BETWEEN :start AND :stop "
        "ORDER BY timestamp ASC;",
        {"addr": address, "start": start.isoformat(), "stop": stop.isoformat()},
    )

    rewards = cur.fetchall()
    for r in rewards:
        tmp = dict()
        tmp["hash"] = r[0]
        tmp["timestamp"] = r[1]
        tmp["gateway"] = r[2]
        tmp["block"] = r[3]
        tmp["amount"] = r[4]
        ret.append(tmp)

    return ret


def _db_reward_put(hash, ts, address, block, amount):
    """
    Save a daily reward total to the db. Does not yet handle errors like an
    existin record
    """
    cur = _DB.cursor()
    cur.execute(
        "INSERT INTO DailyRewards VALUES (:hash, :ts, :addr, :block, :amt)",
        {"hash": hash, "ts": ts, "addr": address, "block": block, "amt": amount},
    )
    _DB.commit()


def oracle_price_at_block(block):
    """
    Return the Helium API oracle price in bones at a given block
    """
    ret = _db_oracle_fetch(block)
    if ret is None:
        url = f"{API_URL}/oracle/prices/{block}"
        ret = _api_request(url)["price"]
        _db_oracle_put(block, ret)
        log.debug(f"Lookup price for block {block} is {ret}")
    else:
        log.debug(f"Cached price for block {block} is {ret}")

    return ret


def hotspot_earnings(address, start, stop):
    """
    Get all earnings [start, end) for the given hotspot
    address.

    Returns
    A list of dicts, one dict for each reward.

    Will return an empty list if the address is not found or no earnings
    were found.
    """
    ret = []

    cached_rewards = _db_reward_fetch(address, start, stop)
    log.debug(f"Got {len(cached_rewards)} cached rewards from DB")
    if len(cached_rewards) > 0:
        cached_max_ts = max([r["timestamp"] for r in cached_rewards])
        # Bump the time just past the cached value
        start = dateparse(cached_max_ts) + timedelta(milliseconds=1)

    url = f"{API_URL}/hotspots/{address}/rewards"
    params = dict()
    params["max_time"] = stop.isoformat()
    params["min_time"] = start.isoformat()
    try:
        fetched_rewards = _api_request(url, params)
        # Weed out duplicate hashes from the fetched list
        cached_hashes = [cr["hash"] for cr in cached_rewards]
        fetched_rewards = [x for x in fetched_rewards if not x["hash"] in cached_hashes]
        for r in fetched_rewards:
            _db_reward_put(r["hash"], r["timestamp"], address, r["block"], r["amount"])
    except:
        pass

    ret.extend(cached_rewards)
    ret.extend(fetched_rewards)
    return ret
