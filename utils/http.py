import httpx
import asyncio

async def async_get(url, retries=3):
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
        except Exception as e:
            if attempt == retries - 1:
                raise e
            await asyncio.sleep(1)
