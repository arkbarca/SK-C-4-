# -*- coding: utf-8 -*-
import pandas as pd
from .table import get_company_color # visualization 폴더 내의 table 모듈에서 import

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

def create_sk_bar_chart(chart_df: pd.DataFrame):
    """SK에너지 강조 막대 차트"""
    if not PLOTLY_AVAILABLE or chart_df.empty: return None
    
    companies = chart_df['회사'].unique()
    color_map = {comp: get_company_color(comp, companies) for comp in companies}
    
    fig = px.bar(
        chart_df, x='구분', y='수치', color='회사',
        title="💼 SK에너지 vs 경쟁사 수익성 지표 비교",
        text='수치', color_discrete_map=color_map, barmode='group', height=450
    )
    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig.update_layout(
        yaxis_title="수치(%)", xaxis_title="재무 지표", legend_title="회사",
        font=dict(family="Malgun Gothic, Apple SD Gothic Neo, sans-serif")
    )
    return fig

def create_sk_radar_chart(chart_df):
    """SK에너지 중심 레이더 차트 (지표별 Min-Max 정규화 적용)"""
    if chart_df.empty or not PLOTLY_AVAILABLE:
        return None
    
    companies = chart_df['회사'].unique() if '회사' in chart_df.columns else []
    metrics = chart_df['구분'].unique() if '구분' in chart_df.columns else []
    
    # 지표별 최소, 최대값 계산
    min_max = {}
    for metric in metrics:
        values = chart_df.loc[chart_df['구분'] == metric, '수치']
        min_val = values.min()
        max_val = values.max()
        # 최소 최대값이 같으면 max_val = min_val + 1로 설정(0 나누기 방지)
        if min_val == max_val:
            max_val = min_val + 1
        min_max[metric] = (min_val, max_val)
    
    fig = go.Figure()
    
    for i, company in enumerate(companies):
        company_data = chart_df[chart_df['회사'] == company] if '회사' in chart_df.columns else chart_df
        normalized_values = []
        for metric in metrics:
            raw_value = company_data.loc[company_data['구분'] == metric, '수치'].values
            if len(raw_value) == 0:
                norm_value = 0
            else:
                val = raw_value[0]
                min_val, max_val = min_max[metric]
                norm_value = (val - min_val) / (max_val - min_val)
            normalized_values.append(norm_value)
        
        # 닫힌 도형을 위해 첫 값 반복
        normalized_values.append(normalized_values[0])
        theta_labels = list(metrics) + [metrics[0]] if len(metrics) > 0 else ['지표1']
        
        # 색상
        color = get_company_color(company, companies)
        
        # SK에너지 스타일 강조
        if 'SK' in company:
            line_width = 5
            marker_size = 12
            name_style = f"**{company}**"
        else:
            line_width = 3
            marker_size = 8
            name_style = company
        
        fig.add_trace(go.Scatterpolar(
            r=normalized_values,
            theta=theta_labels,
            fill='toself',
            name=name_style,
            line=dict(width=line_width, color=color),
            marker=dict(size=marker_size, color=color)
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1],  # 정규화 했으니 0~1 범위
                tickmode='linear',
                tick0=0,
                dtick=0.2,
                tickfont=dict(size=14)
            ),
            angularaxis=dict(
                tickfont=dict(size=16)
            )
        ),
        title="🎯 SK에너지 vs 경쟁사 수익성 지표 비교 (정규화)",
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=14)
        ),
        title_font_size=20,
        font=dict(size=14)
    )
    
    return fig


def create_quarterly_trend_chart(quarterly_df: pd.DataFrame):
    """분기별 추이 혼합 차트"""
    if not PLOTLY_AVAILABLE or quarterly_df.empty: return None

    fig = go.Figure()
    companies = quarterly_df['회사'].unique()

    for company in companies:
        company_data = quarterly_df[quarterly_df['회사'] == company]
        color = get_company_color(company, companies)
        
        # 매출액 (Bar)
        if '매출액(조원)' in company_data.columns:
            fig.add_trace(go.Bar(
                x=company_data['분기'], y=company_data['매출액(조원)'], name=f"{company} 매출액(조)",
                marker_color=color
            ))
    
    fig.update_layout(
        barmode='group', title="📈 분기별 매출액 추이",
        xaxis_title="분기", yaxis_title="매출액 (조원)",
        font=dict(family="Malgun Gothic, Apple SD Gothic Neo, sans-serif")
    )
    return fig

def create_gap_trend_chart(quarterly_df: pd.DataFrame):
    """분기별 갭 추이 차트"""
    if not PLOTLY_AVAILABLE or quarterly_df.empty: return None

    fig = go.Figure()
    companies = quarterly_df['회사'].unique()

    for company in companies:
        company_data = quarterly_df[quarterly_df['회사'] == company]
        color = get_company_color(company, companies)
        
        # 영업이익률 (Line)
        if '영업이익률(%)' in company_data.columns:
            fig.add_trace(go.Scatter(
                x=company_data['분기'], y=company_data['영업이익률(%)'], 
                name=f"{company} 영업이익률(%)",
                mode='lines+markers', line=dict(color=color, width=3),
                marker=dict(size=8)
            ))
    
    fig.update_layout(
        title="📊 분기별 영업이익률 갭 추이",
        xaxis_title="분기", yaxis_title="영업이익률 (%)",
        font=dict(family="Malgun Gothic, Apple SD Gothic Neo, sans-serif"),
        height=450
    )
    return fig

def create_gap_analysis(financial_df: pd.DataFrame, raw_cols: list):
    """SK에너지 대비 경쟁사 갭차이 분석"""
    if financial_df.empty or not raw_cols:
        return pd.DataFrame()
    
    # SK에너지 컬럼 찾기
    sk_col = None
    for col in raw_cols:
        if 'SK에너지' in col:
            sk_col = col
            break
    
    if not sk_col:
        return pd.DataFrame()
    
    gap_analysis = []
    
    for _, row in financial_df.iterrows():
        indicator = row['구분']
        sk_value = row.get(sk_col, 0)
        
        if sk_value == 0:
            continue
            
        gap_data = {'지표': indicator, 'SK에너지': sk_value}
        
        for col in raw_cols:
            if col != sk_col:
                company_name = col.replace('_원시값', '')
                company_value = row.get(col, 0)
                
                # 갭차이 계산 (SK에너지 대비)
                if sk_value != 0:
                    gap_percentage = ((company_value - sk_value) / abs(sk_value)) * 100
                    gap_amount = company_value - sk_value
                else:
                    gap_percentage = 0
                    gap_amount = company_value
                
                gap_data[f'{company_name}_갭(%)'] = round(gap_percentage, 2)
                gap_data[f'{company_name}_갭(금액)'] = gap_amount
                gap_data[f'{company_name}_원본값'] = company_value
        
        gap_analysis.append(gap_data)
    
    return pd.DataFrame(gap_analysis)

def create_gap_chart(gap_analysis_df: pd.DataFrame):
    """갭차이 시각화 차트"""
    if not PLOTLY_AVAILABLE or gap_analysis_df.empty:
        return None
    
    # 갭% 컬럼만 추출
    gap_cols = [col for col in gap_analysis_df.columns if col.endswith('_갭(%)')]
    if not gap_cols:
        return None
    
    # 데이터 준비
    chart_data = []
    for _, row in gap_analysis_df.iterrows():
        indicator = row['지표']
        for col in gap_cols:
            company = col.replace('_갭(%)', '')
            gap_value = row[col]
            chart_data.append({
                '지표': indicator,
                '회사': company,
                '갭(%)': gap_value
            })
    
    chart_df = pd.DataFrame(chart_data)
    
    # 색상 매핑
    companies = chart_df['회사'].unique()
    color_map = {comp: get_company_color(comp, companies) for comp in companies}
    
    fig = px.bar(
        chart_df, x='지표', y='갭(%)', color='회사',
        title="📊 SK에너지 대비 경쟁사 갭차이 분석",
        text='갭(%)', color_discrete_map=color_map, barmode='group', height=500
    )
    
    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig.update_layout(
        yaxis_title="갭차이 (%)", xaxis_title="재무 지표", legend_title="회사",
        font=dict(family="Malgun Gothic, Apple SD Gothic Neo, sans-serif"),
        # 0선 추가
        shapes=[dict(
            type='line', x0=-0.5, x1=len(chart_df['지표'].unique())-0.5, y0=0, y1=0,
            line=dict(color='red', width=2, dash='dash')
        )],
        annotations=[dict(
            x=0.5, y=0, xref='paper', yref='y',
            text='SK에너지 기준선', showarrow=False,
            font=dict(color='red', size=12)
        )]
    )
    
    return fig