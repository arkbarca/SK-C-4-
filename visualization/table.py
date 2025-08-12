# -*- coding: utf-8 -*-
from config import SK_COLORS

def get_company_color(company_name: str, all_companies: list) -> str:
    """회사별 고유 색상 반환 (SK는 빨간색, 경쟁사는 파스텔 구분)"""
    if 'SK' in company_name:
        return SK_COLORS['primary']
    else:
        competitor_colors = [
            SK_COLORS['competitor_green'],  # 파스텔 그린
            SK_COLORS['competitor_blue'],   # 파스텔 블루
            SK_COLORS['competitor_yellow'], # 파스텔 옐로우
            SK_COLORS['competitor_purple'], # 파스텔 퍼플
            SK_COLORS['competitor_orange'], # 파스텔 오렌지
            SK_COLORS['competitor_mint'],   # 파스텔 민트
        ]
        
        non_sk_companies = [c for c in all_companies if 'SK' not in c]
        try:
            idx = non_sk_companies.index(company_name)
            return competitor_colors[idx % len(competitor_colors)]
        except ValueError:
            return SK_COLORS['competitor']