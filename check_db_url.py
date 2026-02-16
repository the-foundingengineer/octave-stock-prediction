import sys
import os
from app.database import engine

print(f"Engine Dialect: {engine.dialect.name}")
print(f"Engine Driver: {engine.driver}")
print(f"URL Scheme: {engine.url.drivername}")

# Also check env var directly
from dotenv import load_dotenv
load_dotenv()
url = os.getenv("DATABASE_URL")
if url:
    print(f"ENV URL starts with: {url[:10]}...")
else:
    print("ENV URL is None")
