# -*- coding: utf-8 -*-
import pandas as pd
from data.loader import DartAPICollector

def prepare_chart_data(df: pd.DataFrame):
    """재무 데이터를 Plotly 차트용 long-format 데이터로 변환"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    ratio_data = df[df['구분'].str.contains('%|점|억원', na=False)].copy()
    companies = [col for col in ratio_data.columns if col != '구분' and not col.endswith('_원시값')]
    
    chart_data_list = []
    for _, row in ratio_data.iterrows():
        for company in companies:
            value_str = str(row.get(company, '0')).replace('%', '').replace('점', '').replace('억원', '')
            try:
                value = float(value_str)
                chart_data_list.append({
                    '지표': row['구분'], '회사': company, '수치': value
                })
            except (ValueError, TypeError):
                continue
    
    return pd.DataFrame(chart_data_list)

def create_dart_source_table(dart_collector: DartAPICollector, collected_companies: list, analysis_year: str):
    """DART 출처 정보 테이블 생성"""
    if not dart_collector.source_tracking:
        return pd.DataFrame()
    
    source_data = []
    for company, info in dart_collector.source_tracking.items():
        if company in collected_companies:
            source_data.append({
                '회사명': company,
                '보고서 유형': info.get('report_type', 'N/A'),
                '연도': info.get('year', analysis_year),
                'DART 바로가기': info.get('dart_url', 'https://dart.fss.or.kr')
            })
    return pd.DataFrame(source_data)