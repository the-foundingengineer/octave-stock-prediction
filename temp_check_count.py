import sys
import os
sys.path.append(os.path.join(os.getcwd(), "app"))
import models
from database import SessionLocal

db = SessionLocal()
try:
    count = db.query(models.DailyKline).count()
    with open("count_output.txt", "w") as f:
        f.write(str(count))
except Exception as e:
    with open("count_output.txt", "w") as f:
        f.write(f"Error: {e}")
finally:
    db.close()
