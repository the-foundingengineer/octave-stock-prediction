import asyncio
from app.database import SessionLocal
from app.models import Stock
from app.news_service import update_stock_news

async def update_all_stocks():

    db = SessionLocal()

    try:
        stocks = db.query(Stock).all()

        # Batch to avoid API rate limits
        batch_size = 10
        for i in range(0, len(stocks), batch_size):

            batch = stocks[i:i+batch_size]

            tasks = [
                update_stock_news(db, stock)
                for stock in batch
            ]

            await asyncio.gather(*tasks)

            await asyncio.sleep(2)  # avoid rate limit

    finally:
        db.close()


async def main():
    while True:
        await update_all_stocks()
        await asyncio.sleep(900)  # 15 mins


if __name__ == "__main__":
    asyncio.run(main())