import aiohttp
import hashlib
import logging
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator
import tldextract as tld
import random
import json
import asyncio
from aiohttp_socks import ProxyConnector
from aiohttp import ClientSession, ClientTimeout

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

DEFAULT_OLDNESS_SECONDS = 3600 * 3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10
BASE_TIMEOUT = 30

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

logging.basicConfig(level=logging.INFO)

async def fetch_data(url, proxy, headers=None):
    connector = ProxyConnector.from_url(f"socks5://{proxy[0]}:{proxy[1]}", rdns=True)
    timeout = ClientTimeout(total=BASE_TIMEOUT)
    try:
        async with ClientSession(connector=connector, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                if response.status == 200:
                    json_data = await response.json(content_type=None)  # Manually handle content type
                    logging.info(f"Successfully fetched data with proxy {proxy}")
                    return json_data
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching data with proxy {proxy}: {e}")
        return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
    return max_oldness_seconds, maximum_items_to_collect, min_post_length

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    proxies = load_proxies('/exorde/ips.txt')
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
    yielded_items = 0

    while yielded_items < maximum_items_to_collect:
        proxy = random.choice(proxies)
        logging.info(f"Using proxy {proxy}")
        data = await fetch_data(feed_url, proxy)

        if not data:
            await asyncio.sleep(1)  # Add delay before retrying
            continue

        logging.info(f"Total data fetched: {len(data)}")

        sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
        filtered_data = []
        for entry in sorted_data:
            pub_date = convert_to_standard_timezone(entry["pubDate"])
            if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
                filtered_data.append(entry)
            else:
                logging.info(f"Entry {entry['title']} with date {entry['pubDate']} is too old.")

        logging.info(f"[News stream collector] Filtered data : {len(filtered_data)}")

        if len(filtered_data) == 0:
            logging.info("No data found within the specified timeframe and length.")
            await asyncio.sleep(1)  # Add delay before retrying
            continue

        filtered_data = random.sample(filtered_data, int(len(filtered_data) * 0.3))

        successive_old_entries = 0

        for entry in filtered_data:
            if random.random() < 0.25:
                continue
            logging.info(f"[News stream collector] Processing entry: {entry['title']} - {entry['pubDate']} - {entry['source_url']}")
            if yielded_items >= maximum_items_to_collect:
                break

            pub_date = convert_to_standard_timezone(entry["pubDate"])

            if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
                sha1 = hashlib.sha1()
                author = entry["creator"][0] if entry.get("creator") else "anonymous"
                sha1.update(author.encode())
                author_sha1_hex = sha1.hexdigest()

                content_article_str = ""
                if entry.get("content"):
                    content_article_str = entry["content"]
                elif entry.get("description"):
                    content_article_str = entry["description"]
                else:
                    content_article_str = entry["title"]

                domain_str = entry["source_url"] if entry.get("source_url") else "unknown"
                domain_str = tld.extract(domain_str).registered_domain

                logging.info(f"[News stream collector] Entry content length: {len(content_article_str)}")

                new_item = Item(
                    content=Content(str(content_article_str)),
                    author=Author(str(author_sha1_hex)),
                    created_at=CreatedAt(pub_date),
                    title=Title(entry["title"]),
                    domain=Domain(str(domain_str)),
                    url=Url(entry["link"]),
                    external_id=ExternalId(entry["article_id"])
                )

                if len(new_item.content) >= min_post_length:
                    yielded_items += 1
                    yield new_item
                else:
                    logging.info(f"[News stream collector] Skipping entry with content length {len(new_item.content)}")
            else:
                dt = datett.strptime(pub_date, "%Y-%m-%dT%H:%M:%S.00Z")
                dt = dt.replace(tzinfo=timezone.utc)
                current_dt = datett.now(timezone.utc)
                time_diff = current_dt - dt
                logging.info(f"[News stream collector] Entry is {abs(time_diff)} old : skipping.")
                
                successive_old_entries += 1
                if successive_old_entries >= 5:
                    logging.info(f"[News stream collector] Too many old entries. Stopping.")
                    break

        await asyncio.sleep(1)  # Add delay before retrying
    logging.info(f"[News stream collector] Done.")

def load_proxies(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    proxies = []
    for line in lines:
        ip_port = line.strip().split('=')[1]
        ip, port = ip_port.split(':')
        proxies.append((ip, port))
    return proxies

# Ensure you have 'aiohttp_socks' installed to use the ProxyConnector.
# You can install it using pip: pip install aiohttp_socks
