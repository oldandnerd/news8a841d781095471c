from news8a841d781095471c import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    try:
        # Example parameters dictionary
        parameters = {
            "max_oldness_seconds":1200,
            "maximum_items_to_collect": 3
        }
        async for item in query(parameters):
            assert isinstance(item, Item)
            print("\n")
            print("Title = ",item['title'])
            print("Date = ",item['created_at'])
            print("Content = ",item['content'])
            print("author = ",item['author'])
            print("url = ",item['url'])
            print("external_id = ",item['external_id'])
    except ValueError as e:
        print(f"Error: {str(e)}")
