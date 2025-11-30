from datetime import datetime, timezone
from src.extractor.utils import safe_request
from sqlalchemy.exc import SQLAlchemyError
from src.db import Metric, SessionLocal

def fetch_crypto(**context):
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    
    payload, response_time_ms, errors = safe_request(url, params=params)
    
    if payload and "bitcoin" in payload:
        bitcoin_data = payload["bitcoin"]
        price_usd = bitcoin_data.get("usd")
        volume_24h = bitcoin_data.get("usd_24h_vol")
        change_pct_24h = bitcoin_data.get("usd_24h_change")
    else:
        price_usd = None
        volume_24h = None
        change_pct_24h = None

    with SessionLocal() as session:
        try:
            entry = Metric(
                collected_at=datetime.now(timezone.utc),
                source="crypto",
                crypto_price_usd=price_usd,
                crypto_volume_24h=volume_24h,
                crypto_change_pct_24h=change_pct_24h,
                
                news_num_articles=None,
                news_avg_title_len=None,
                news_sentiment=None,
                weather_temp_c=None,
                weather_humidity=None,
                weather_pressure=None,
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