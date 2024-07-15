import aiohttp
import aiofiles
import hashlib
import logging
from datetime import datetime as datett, timezone
from dateutil import parser
from typing import AsyncGenerator, Set
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

# Constants
DEFAULT_OLDNESS_SECONDS = 3600 * 3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10
BASE_TIMEOUT = 30
FETCH_DELAY = 5  # Delay between fetch attempts
RESET_INTERVAL = 86400  # 24 hours in seconds

# User Agent List
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

# Logging configuration
logging.basicConfig(level=logging.INFO)

async def fetch_data(url, proxy, headers=None):
    """Fetch data from the URL using a specified proxy."""
    connector = ProxyConnector.from_url(f"socks5://{proxy[0]}:{proxy[1]}", rdns=True)
    timeout = ClientTimeout(total=BASE_TIMEOUT)
    
    try:
        async with ClientSession(connector=connector, headers=headers or {"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=timeout) as session:
            async with session.get(url) as response:
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

def convert_to_standard_timezone(dt_str):
    """Convert datetime string to standard timezone format."""
    dt = parser.parse(dt_str)
    return dt.astimezone(timezone.utc)

def is_within_timeframe(dt, timeframe_sec):
    """Check if the datetime is within the specified timeframe."""
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff.total_seconds()) <= timeframe_sec

def read_parameters(parameters):
    """Read and set default parameters."""
    max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
    min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    return max_oldness_seconds, maximum_items_to_collect, min_post_length

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    """Query the feed and yield items."""
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    proxies = [proxy async for proxy in load_proxies('/exorde/ips.txt')]
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")

    yielded_items = 0
    queried_article_ids: Set[str] = set()  # Track queried article IDs
    last_reset_time = datett.now(timezone.utc)

    while yielded_items < maximum_items_to_collect:
        # Reset queried_article_ids every 24 hours
        current_time = datett.now(timezone.utc)
        if (current_time - last_reset_time).total_seconds() > RESET_INTERVAL:
            queried_article_ids.clear()
            last_reset_time = current_time
            logging.info("Reset queried_article_ids set.")

        proxy = random.choice(proxies)
        logging.info(f"Using proxy {proxy}")
        data = await fetch_data(feed_url, proxy)

        if not data:
            await asyncio.sleep(FETCH_DELAY)  # Add delay before retrying
            continue

        logging.info(f"Total data fetched: {len(data)}")

        # Sort data by publication date in descending order to process newest first
        sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
        filtered_data = [
            entry for entry in sorted_data
            if is_within_timeframe(convert_to_standard_timezone(entry["pubDate"]), max_oldness_seconds)
            and entry["article_id"] not in queried_article_ids
        ]

        for entry in filtered_data:
            queried_article_ids.add(entry["article_id"])  # Add to queried IDs set

        logging.info(f"[News stream collector] Filtered data: {len(filtered_data)}")

        if len(filtered_data) == 0:
            logging.info("No data found within the specified timeframe and length.")
            await asyncio.sleep(FETCH_DELAY)  # Add delay before retrying
            continue

        filtered_data = random.sample(filtered_data, int(len(filtered_data) * 0.3))

        for entry in filtered_data:
            if random.random() < 0.25:
                continue
            if yielded_items >= maximum_items_to_collect:
                break

            sha1 = hashlib.sha1()
            author = entry["creator"][0] if entry.get("creator") else "anonymous"
            sha1.update(author.encode())
            author_sha1_hex = sha1.hexdigest()

            content_article_str = entry.get("content") or entry.get("description") or entry["title"]
            domain_str = tld.extract(entry["source_url"] or "unknown").registered_domain

            new_item = Item(
                content=Content(str(content_article_str)),
                author=Author(str(author_sha1_hex)),
                created_at=CreatedAt(convert_to_standard_timezone(entry["pubDate"]).strftime("%Y-%m-%dT%H:%M:%S.00Z")),
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

        await asyncio.sleep(FETCH_DELAY)  # Add delay before retrying
    logging.info(f"[News stream collector] Done.")

async def load_proxies(file_path):
    """Load proxies from a file asynchronously."""
    async with aiofiles.open(file_path, 'r') as file:
        async for line in file:
            ip_port = line.strip().split('=')[1]
            ip, port = ip_port.split(':')
            yield (ip, port)

# Ensure you have 'aiohttp_socks' installed to use the ProxyConnector.
# You can install it using pip: pip install aiohttp_socks
