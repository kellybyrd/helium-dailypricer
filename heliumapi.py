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


def _api_request(url, query_params=dict()):
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
    ret = list()
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


def _db_reward_put_many(address, rewards):
    """
    Save a several reward records to the DB.

    I probably should be doing this with executemany(), but it felt like transforming
    a large rewards list-of-dicts into a list of properly ordered tuples was as much
    work as calling execute() a bunch between commit()
    """
    # Break rows up into chunks of 50 rows. See:
    # https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    CHUNK_SIZE = 50
    chunks = [rewards[i:i + CHUNK_SIZE] for i in range(0, len(rewards), CHUNK_SIZE)]
    cur = _DB.cursor()

    for chunk in chunks:
        # There's a constraint on hash, but code that calls this intentionally overlaps
        # the times it fetches from the API with time should be in the DB in order to be
        # sure it doesn't miss anything. We're using REPLACE here so we can be lazy and
        # not deal with CONSTRAINT viloations.
        for r in chunk:
            cur.execute(
                "REPLACE INTO DailyRewards VALUES (:hash, :ts, :addr, :block, :amt)",
                {"hash": r["hash"], "ts": r["timestamp"], "addr": address, "block": r["block"],
                "amt": r["amount"]},
            )
        # commit per chunk, not per execute.
        _DB.commit()


def _db_reward_put(hash, ts, address, block, amount):
    """
    Save a daily reward total to the db. Does not yet handle errors like an
    existin record
    """
    cur = _DB.cursor()
    # There's a constraint on hash, but code that calls this intentionally overlaps
    # the times it fetches from the API with time should be in the DB in order to be
    # sure it doesn't miss anything. We're using REPLACE here so we can be lazy and
    # not deal with CONSTRAINT viloations.
    cur.execute(
        "REPLACE INTO DailyRewards VALUES (:hash, :ts, :addr, :block, :amt)",
        {"hash": hash, "ts": ts, "addr": address, "block": block, "amt": amount},
    )
    _DB.commit()


def _db_reward_max_min(address):
    """
    Given an address, get the oldest and newest reward timestamps.
    """
    ts_min = None
    ts_max = None

    cur = _DB.cursor()
    cur.execute("SELECT MIN(timestamp) FROM DailyRewards WHERE address=:addr;",
                {"addr": address})
    result = cur.fetchone()[0]
    if result is not None:
        ts_min = dateparse(result)

    cur.execute("SELECT MAX(timestamp) FROM DailyRewards WHERE address=:addr;",
                {"addr": address})
    result = cur.fetchone()[0]
    if result is not None:
        ts_max = dateparse(result)

    return (ts_min, ts_max)


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


def _api_reward_fetch(address, start, stop):
    # Handle paged results and put items in the DB
    ret = list()
    url = f"{API_URL}/hotspots/{address}/rewards"
    params = dict()
    params["max_time"] = stop.isoformat()
    params["min_time"] = start.isoformat()
    try:
        ret = _api_request(url, params)
    except:
        pass

    log.debug(f"_api_reward_fetch: putting {len(ret)} records in the DB")
    _db_reward_put_many(address, ret)

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
    ONE_SEC = timedelta(
        seconds=1
    )  # Added to API fetch times to ensure we overlap a bit

    # The overall strategy is to ensure no holes in the date range of the db cache.
    # To do this we always try to extend the cache, even if the start/stop params do
    # not overlap with the cached data.
    # * Get db_min and db_max
    # * If start < db_min: api_fetch for start to db_min(+1sec)
    #   Insert these into db. At this point db range is start to db_max
    # * If stop > db_max: api_fetch for db_max(-1sec) to stop
    #   Insert these into db. At this point db range will be at least start to stop.
    # * Read start to stop from DB
    (db_min_ts, db_max_ts) = _db_reward_max_min(address)
    if db_min_ts is None:
        # Nothing in the DB yet
        log.debug(f"DB: empty")
        _api_reward_fetch(address, start, stop)
    else:
        if start < db_min_ts:
            # Need data earlier than range in db
            log.debug(f"DB: fetch before")
            _api_reward_fetch(address, start, db_min_ts + ONE_SEC)
        if stop > db_max_ts:
            # Need data later than range in db
            log.debug(f"DB: fetch after")
            _api_reward_fetch(address, db_max_ts - ONE_SEC, stop)

    # The DB now covers the time range we need, so fetch it from there.
    return _db_reward_fetch(address, start, stop)
