import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("DATABASE_URL")
print(f"Connecting to: {url.split('@')[-1]}") # Print host only for security

try:
    engine = create_engine(url)
    with engine.connect() as conn:
        print("Successfully connected to the database!")
except Exception as e:
    print(f"Failed to connect: {e}")
