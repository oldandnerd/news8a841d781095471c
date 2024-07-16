import aiohttp
import hashlib
import logging
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator
import tldextract as tld
import random

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
)

DEFAULT_OLDNESS_SECONDS = 3600*3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10

async def fetch_data(url, headers=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json(content_type=None)  # Manually handle content type
            else:
                logging.error(f"Error fetching data: {response.status} {await response.text()}")
                return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    return abs(datett.now(timezone.utc) - dt) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        return (
            parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS),
            parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS),
            parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        )
    return DEFAULT_OLDNESS_SECONDS, DEFAULT_MAXIMUM_ITEMS, DEFAULT_MIN_POST_LENGTH

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    data = await fetch_data(feed_url)

    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")

    sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
    filtered_data = [entry for entry in sorted_data if is_within_timeframe_seconds(convert_to_standard_timezone(entry["pubDate"]), max_oldness_seconds)]

    logging.info(f"[News stream collector] Filtered data : {len(filtered_data)}")
    sampled_data = random.sample(filtered_data, min(len(filtered_data), int(len(filtered_data) * 0.3)))

    yielded_items = 0
    successive_old_entries = 0

    for entry in sampled_data:
        if yielded_items >= maximum_items_to_collect:
            break

        pub_date = convert_to_standard_timezone(entry["pubDate"])

        if not is_within_timeframe_seconds(pub_date, max_oldness_seconds):
            successive_old_entries += 1
            if successive_old_entries >= 5:
                logging.info(f"[News stream collector] Too many old entries. Stopping.")
                break
            continue

        successive_old_entries = 0

        sha1 = hashlib.sha1()
        author = entry.get("creator", ["anonymous"])[0]
        sha1.update(author.encode())
        author_sha1_hex = sha1.hexdigest()

        content_article_str = entry.get("content") or entry.get("description") or entry.get("title")
        domain_str = tld.extract(entry.get("source_url", "unknown")).registered_domain

        new_item = Item(
            content=Content(str(content_article_str)),
            author=Author(str(author_sha1_hex)),
            created_at=CreatedAt(pub_date),
            title=Title(entry["title"]),
            domain=Domain(str(domain_str)),
            url=Url(entry["link"]),
            external_id=ExternalId(entry["article_id"])
        )

        yielded_items += 1
        yield new_item

    logging.info(f"[News stream collector] Done.")
