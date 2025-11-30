from datetime import datetime, timezone
from src.extractor.utils import safe_request
from sqlalchemy.exc import SQLAlchemyError
from src.db import Metric, SessionLocal

def fetch_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": 55.75, "longitude": 37.61, "current_weather": True}

    payload, response_time_ms, errors = safe_request(url, params=params)

    if payload and "current_weather" in payload:
        cw = payload["current_weather"]
        temp = cw.get("temperature")
        windspeed = cw.get("windspeed")
        pressure = cw.get("pressure", 1013)
    else:
        temp = None
        windspeed = None
        pressure = None

    with SessionLocal() as session:
        try:
            entry = Metric(
                collected_at=datetime.now(timezone.utc),
                source="weather",
                weather_temp_c=temp,
                weather_humidity=windspeed,
                weather_pressure=pressure,

                news_num_articles=None,
                news_avg_title_len=None,
                news_sentiment=None,
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