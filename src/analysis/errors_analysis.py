import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sqlalchemy import text
import numpy as np
from airflow.models import Variable

from src.db import SessionLocal, engine
from src.analysis.utils import get_data_for_analysis

def analyze_response_times_and_errors(hours=2):
    df = get_data_for_analysis(hours)
    
    if df.empty:
        return {
            'analysis_time': datetime.now().isoformat(),
            'data_points': 0,
            'error': 'Нет данных для анализа'
        }
    
    analysis = {
        'analysis_time': datetime.now().isoformat(),
        'data_points': len(df),
        'time_period_hours': hours,
    }
    
    if 'response_time_ms' in df.columns:
        response_times = df['response_time_ms'].dropna()
        if not response_times.empty:
            analysis['response_time_stats'] = {
                'count': int(response_times.count()),
                'mean_ms': float(response_times.mean()),
                'median_ms': float(response_times.median()),
                'max_ms': float(response_times.max()),
                'min_ms': float(response_times.min()),
                'std_ms': float(response_times.std())
            }
    
    if 'errors_count' in df.columns:
        errors = df['errors_count'].dropna()
        if not errors.empty:
            analysis['error_stats'] = {
                'total_errors': int(errors.sum()),
                'avg_errors_per_request': float(errors.mean()),
                'max_errors_per_request': int(errors.max()),
                'requests_with_errors': int((errors > 0).sum()),
                'error_rate_percent': float((errors > 0).mean() * 100)
            }
            
            if analysis['error_stats']['total_errors'] > 10:
                analysis['pipeline_should_stop'] = True
                analysis['stop_reason'] = f"Количество ошибок ({analysis['error_stats']['total_errors']}) превышает 10 за {hours} часов"
            else:
                analysis['pipeline_should_stop'] = False
    
    return analysis

def run_performance_analysis():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = f"/opt/airflow/reports/performance_{timestamp}"
    os.makedirs(reports_dir, exist_ok=True)
    
    analysis = analyze_response_times_and_errors(hours=2)
    
    analysis_file = os.path.join(reports_dir, "performance_analysis.txt")
    with open(analysis_file, 'w', encoding='utf-8') as f:
        f.write("АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ\n")
        f.write("=" * 50 + "\n")
        f.write(f"Время анализа: {analysis['analysis_time']}\n")
        f.write(f"Период данных: последние {analysis['time_period_hours']} часа\n")
        f.write(f"Количество записей: {analysis['data_points']}\n\n")
        
        if 'error' in analysis:
            f.write(f"Ошибка: {analysis['error']}\n")
        else:
            if 'response_time_stats' in analysis:
                f.write("СТАТИСТИКИ ВРЕМЕНИ ОТВЕТА\n")
                f.write("-" * 30 + "\n")
                for key, value in analysis['response_time_stats'].items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
            
            if 'error_stats' in analysis:
                f.write("СТАТИСТИКИ ОШИБОК\n")
                f.write("-" * 30 + "\n")
                for key, value in analysis['error_stats'].items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
            
            if analysis.get('pipeline_should_stop'):
                f.write("ПАЙПЛАЙН ДОЛЖЕН БЫТЬ ОСТАНОВЛЕН\n")
                f.write(f"Причина: {analysis['stop_reason']}\n")
            else:
                f.write("Пайплайн может продолжать работу\n")
    
    return analysis.get('pipeline_should_stop', False)

def check_pipeline_stop_condition(**context):
    try:
        should_stop = run_performance_analysis()
        
        if should_stop:
            Variable.set("pipeline_should_stop", "true")
            Variable.set("pipeline_stop_reason", f"Анализ показал превышение лимита ошибок в {datetime.now()}")
            return True
        else:
            Variable.set("pipeline_should_stop", "false")
            return False
            
    except Exception as e:
        Variable.set("pipeline_should_stop", "error")
        Variable.set("pipeline_stop_reason", f"Ошибка анализа: {str(e)}")
        return False