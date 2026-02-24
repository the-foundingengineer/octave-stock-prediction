import httpx
from app.models import NewsArticle
from app.config import NEWS_API_KEY
from datetime import datetime

BASE_URL = "https://eventregistry.org/api/v1/article/getArticles"


async def fetch_news(company_name: str, page: int = 1, count: int = 20):
    """
    Fetch news articles from Event Registry (newsapi.ai) for a given company name.

    Request body follows the Event Registry /api/v1/article/getArticles spec:
    - POST with JSON body
    - Returns: { articles: { results: [...], totalResults, page, count, pages } }

    Each article in results has:
      uri, lang, date, time, dateTime, dateTimePub, dataType,
      url, title, body, source: { uri, title },
      image, sentiment, isDuplicate, ...
      {
  "action": "getArticles",
  "conceptUri": "http://en.wikipedia.org/wiki/George_Clooney",
  "sourceLocationUri": [
    "http://en.wikipedia.org/wiki/Germany",
    "http://en.wikipedia.org/wiki/Los_Angeles_County,_California"
  ],
  "articlesPage": 1,
  "articlesCount": 100,
  "articlesSortBy": "date",
  "articlesSortByAsc": false,
  "articlesArticleBodyLen": -1,
  "resultType": "articles",
  "apiKey": "YOUR_API_KEY",
  "forceMaxDataTimeWindow": 31
}
    """
    payload = {
        "action": "getArticles",
        "keyword": company_name,
        "articlesPage": page,
        "articlesCount": count,
        "articlesSortBy": "date",
        "articlesSortByAsc": False,
        "dataType": ["news", "pr"],
        "forceMaxDataTimeWindow": 31,
        "resultType": "articles",
        "apiKey": NEWS_API_KEY,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(BASE_URL, json=payload)
        response.raise_for_status()
        return response.json()


async def update_stock_news(db, stock):
    """
    Fetch and persist news articles for a single stock.
    `stock` must have `.name` and `.id` attributes.
    """
    try:
        data = await fetch_news(stock.name)
    except Exception as e:
        # Don't crash the whole scheduler job if one stock fails
        return

    articles = data.get("articles", {}).get("results", [])

    for article in articles:
        url = article.get("url")
        if not url:
            continue

        # Skip articles Event Registry flags as duplicates
        if article.get("isDuplicate", False):
            continue

        # Skip duplicates already in DB
        if db.query(NewsArticle).filter(NewsArticle.url == url).first():
            continue

        # Parse dateTime → datetime object
        raw_dt = article.get("dateTime") or article.get("dateTimePub")
        published_at = None
        if raw_dt:
            try:
                published_at = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            except ValueError:
                published_at = None

        source_name = article.get("source", {}).get("title")

        news = NewsArticle(
            stock_id=stock.id,
            title=article.get("title", ""),
            content=article.get("body"),
            url=url,
            source=source_name,
            image_url=article.get("image"),
            lang=article.get("lang"),
            event_uri=article.get("eventUri"),
            news_type=article.get("dataType"),
            sentiment=article.get("sentiment"),
            wgt=article.get("wgt"),
            published_at=published_at,
        )

        db.add(news)

    db.commit()