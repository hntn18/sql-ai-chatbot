import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    """
    Configuration settings loaded from environment variables
    """
    # Database Configuration
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "localhost")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "5432")
    DATABASE: str = os.getenv("DATABASE", "sqlai")
    DATABASE_USERNAME: str = os.getenv("DATABASE_USERNAME", "postgres")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "postgres")
    
    # API Configuration
    EAS_SERVICE_TOKEN: str = os.getenv("EAS_SERVICE_TOKEN", "")
    EAS_SERVICE_URL: str = os.getenv("EAS_SERVICE_URL", "")

# Create a single instance to be imported
settings = Settings()
