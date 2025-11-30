import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sqlalchemy import text
import numpy as np

from src.db import SessionLocal, engine
from src.analysis.utils import get_data_for_analysis

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def calculate_basic_statistics(df: pd.DataFrame):
    if df.empty:
        return {}
    
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_columns = [col for col in numeric_columns if col != 'id']
    
    stats = {}
    for col in numeric_columns:
        if col in df.columns:
            stats[col] = {
                'count': int(df[col].count()),
                'mean': float(df[col].mean()) if df[col].count() > 0 else None,
                'median': float(df[col].median()) if df[col].count() > 0 else None,
                'std': float(df[col].std()) if df[col].count() > 0 else None,
                'min': float(df[col].min()) if df[col].count() > 0 else None,
                'max': float(df[col].max()) if df[col].count() > 0 else None,
                'q25': float(df[col].quantile(0.25)) if df[col].count() > 0 else None,
                'q75': float(df[col].quantile(0.75)) if df[col].count() > 0 else None,
            }
    
    return stats

def create_distribution_plots(df, output_dir):
    if df.empty:
        return
    
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_columns = [col for col in numeric_columns if col != 'id']
    
    for col in numeric_columns:
        if col in df.columns and df[col].count() > 0:
            try:
                fig, axes = plt.subplots(2, 2, figsize=(15, 10))
                fig.suptitle(f'Анализ метрики: {col}', fontsize=16)
                
                axes[0, 0].hist(df[col].dropna(), bins=30, alpha=0.7, color='skyblue')
                axes[0, 0].set_title('Распределение')
                axes[0, 0].set_xlabel(col)
                axes[0, 0].set_ylabel('Частота')
                
                axes[0, 1].boxplot(df[col].dropna())
                axes[0, 1].set_title('Box Plot')
                axes[0, 1].set_ylabel(col)
                
                if 'collected_at' in df.columns:
                    df_time = df[['collected_at', col]].dropna()
                    if not df_time.empty:
                        axes[1, 0].plot(df_time['collected_at'], df_time[col], marker='o', alpha=0.7)
                        axes[1, 0].set_title('Динамика')
                        axes[1, 0].set_xlabel('Время')
                        axes[1, 0].set_ylabel(col)
                        axes[1, 0].tick_params(axis='x', rotation=45)
                
                axes[1, 1].text(0.1, 0.5, f'Count: {df[col].count()}\n'
                                          f'Mean: {df[col].mean():.2f}\n'
                                          f'Std: {df[col].std():.2f}\n'
                                          f'Min: {df[col].min():.2f}\n'
                                          f'Max: {df[col].max():.2f}',
                                transform=axes[1, 1].transAxes, fontsize=12,
                                verticalalignment='center')
                axes[1, 1].set_title('Статистики')
                axes[1, 1].axis('off')
                
                plt.tight_layout()
                
                plt.savefig(os.path.join(output_dir, f'distribution_{col}.png'))
                plt.close()
            except Exception as e:
                print(f"Ошибка при создании графика для {col}: {e}")

def run_descriptive_analysis():  
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = f"/opt/airflow/reports/descriptive_{timestamp}"
    os.makedirs(reports_dir, exist_ok=True)
    
    df = get_data_for_analysis(hours=1)
    
    if df.empty:
        print("Нет данных для анализа")
        return False
    
    stats = calculate_basic_statistics(df)
    
    stats_file = os.path.join(reports_dir, "basic_statistics.txt")
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("БАЗОВЫЕ СТАТИСТИКИ\n")
        f.write("=" * 50 + "\n")
        f.write(f"Время анализа: {datetime.now().isoformat()}\n")
        f.write(f"Период данных: последний час\n")
        f.write(f"Количество записей: {len(df)}\n\n")
        
        for metric, metric_stats in stats.items():
            f.write(f"\nМетрика: {metric}\n")
            f.write("-" * 30 + "\n")
            for stat_name, stat_value in metric_stats.items():
                if stat_value is not None:
                    f.write(f"{stat_name}: {stat_value}\n")
                else:
                    f.write(f"{stat_name}: N/A\n")
    create_distribution_plots(df, reports_dir)
    
    return True