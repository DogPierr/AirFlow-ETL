"""
Модуль для работы с базой данных
"""

import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:{os.getenv('POSTGRES_PASSWORD', 'airflow')}@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'airflow')}"

engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Metric(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)
    source = Column(String, index=True)

    news_num_articles = Column(Integer, nullable=True)
    news_avg_title_len = Column(Float, nullable=True)
    news_sentiment = Column(Float, nullable=True)

    weather_temp_c = Column(Float, nullable=True)
    weather_humidity = Column(Float, nullable=True)
    weather_pressure = Column(Float, nullable=True)

    crypto_price_usd = Column(Float, nullable=True)
    crypto_volume_24h = Column(Float, nullable=True)
    crypto_change_pct_24h = Column(Float, nullable=True)

    custom_metric = Column(Float, nullable=True)

    response_time_ms = Column(Integer, nullable=True)
    errors_count = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print("База данных успешно инициализирована")
        return True
    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")
        return False

def close_db():
    engine.dispose()
