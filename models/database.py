# from motor.motor_asyncio import AsyncIOMotorClient
# import os
# from config import settings

# client = AsyncIOMotorClient(os.getenv("MONGO_URL"))
# db = client["agentdb"]


from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(os.getenv("MONGO_URL"))
        self.db = self.client.agentdb

    async def get_collection(self, name: str):
        return self.db[name]

db = Database()