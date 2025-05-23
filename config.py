from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Subcontractor Research Agent"
    redis_url: str
    mongo_url: str 
    class Config:
        env_file = ".env"

settings = Settings()
