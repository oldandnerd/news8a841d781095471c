import aiohttp
import hashlib
import logging
import asyncio
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

async def fetch_data(url, headers=None):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                if response.status == 200:
                    json_data = await response.json(content_type=None)  # Manually handle content type
                    return json_data
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
    feed_url = f'http://news_server:8000/get_articles?size={maximum_items_to_collect}'  # Fetch more initially
    stored_data = []

    async def refill_data():
        nonlocal stored_data
        logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
        data = await fetch_data(feed_url)
        if data:
            stored_data = random.sample(data, len(data))  # Shuffle data and store it
            logging.info(f"[News stream collector] Total data received and stored: {len(stored_data)}")
        else:
            logging.warning("Fetched data is empty. Retrying after 60 seconds.")
            await asyncio.sleep(60)
    
    # Initial fill
    await refill_data()

    yielded_items = 0

    while yielded_items < maximum_items_to_collect:
        if not stored_data:
            await refill_data()
            if not stored_data:  # If still empty after refilling, exit loop
                logging.warning("No more data available after refill attempt.")
                break

        entry = stored_data.pop()
        
        logging.info(f"[News stream collector] Processing entry: {entry['title']} - {entry['pubDate']} - {entry['source_url']}")

        pub_date = convert_to_standard_timezone(entry["pubDate"])

        sha1 = hashlib.sha1()
        author = entry["creator"][0] if entry.get("creator") else "anonymous"
        sha1.update(author.encode())
        author_sha1_hex = sha1.hexdigest()

        content_article_str = entry.get("content") or entry.get("description") or entry["title"]

        domain_str = entry["source_url"] if entry.get("source_url") else "unknown"
        domain_str = tld.extract(domain_str).registered_domain

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
