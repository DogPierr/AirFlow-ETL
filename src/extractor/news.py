from datetime import datetime, timezone
from src.extractor.utils import safe_request
from sqlalchemy.exc import SQLAlchemyError
from src.db import Metric, SessionLocal

def fetch_news(**context):
    url = "https://hn.algolia.com/api/v1/search?query=ai"
    payload, response_time_ms, errors = safe_request(url)

    if payload and "hits" in payload:
        articles = payload["hits"]
        num_articles = len(articles)
        avg_title_len = (
            sum(len(a.get("title", "")) for a in articles) / num_articles
            if num_articles > 0 else 0
        )
        sentiment = 0.0
    else:
        num_articles = 0
        avg_title_len = 0
        sentiment = 0.0

    with SessionLocal() as session:
        try:
            entry = Metric(
                collected_at=datetime.now(timezone.utc),
                source="news",
                news_num_articles=num_articles,
                news_avg_title_len=avg_title_len,
                news_sentiment=sentiment,
  
                weather_temp_c=None,
                weather_humidity=None,
                weather_pressure=None,
                crypto_price_usd=None,
                crypto_volume_24h=None,
                crypto_change_pct_24h=None,
                custom_metric=None,

                response_time_ms=response_time_ms,
                errors_count=errors,
            )
            session.add(entry)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print("DB error:", e)

    return True