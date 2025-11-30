import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sqlalchemy import text
import numpy as np

from src.db import SessionLocal, engine

def get_data_for_analysis(hours=1):
    with SessionLocal() as session:
        try:
            query = text("""
                SELECT * FROM metrics 
                WHERE collected_at >= NOW() - INTERVAL ':hours hours'
                ORDER BY collected_at DESC
            """)
            df = pd.read_sql(query, engine, params={'hours': hours})
            return df
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return pd.DataFrame()