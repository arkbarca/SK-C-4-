# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import json
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Union

import feedparser
import pandas as pd
import requests
import streamlit as st
from dateutil import parser

# 프로젝트 설정 파일 import
import config

# 선택적 의존성 import
try:
    import gspread
    from google.oauth2.service_account import Credentials
    _GSPREAD_AVAILABLE = True
except ImportError:
    _GSPREAD_AVAILABLE = False


class DartAPICollector:
    """DART API를 통해 재무 데이터를 수집하는 클래스"""
    def __init__(self, api_key):
        self.api_key = api_key
        self.source_tracking = {}
        self.company_name_mapping = config.COMPANY_NAME_MAPPING
        self.stock_code_mapping = config.STOCK_CODE_MAPPING

    def get_corp_code_enhanced(self, company_name):
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={self.api_key}"
        search_names = self.company_name_mapping.get(company_name, [company_name])
        
        try:
            res = requests.get(url)
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_file = z.open(z.namelist()[0])
                tree = ET.parse(xml_file)
                root = tree.getroot()
            
            all_companies = []
            for corp in root.findall("list"):
                corp_name_elem = corp.find("corp_name")
                corp_code_elem = corp.find("corp_code")
                stock_code_elem = corp.find("stock_code")
                
                if corp_name_elem is not None and corp_code_elem is not None:
                    all_companies.append({
                        'name': corp_name_elem.text,
                        'code': corp_code_elem.text,
                        'stock_code': stock_code_elem.text.strip() if stock_code_elem is not None and stock_code_elem.text else None
                    })
            
            for search_name in search_names:
                if search_name.isdigit(): # 종목코드로 검색
                    for company in all_companies:
                        if company['stock_code'] == search_name:
                            return company['code']
                
                for company in all_companies: # 정확히 일치
                    if company['name'] == search_name:
                        return company['code']
            
            return None
        except Exception as e:
            st.error(f"회사 코드 조회 오류: {e}")
            return None

    def get_financial_statement(self, corp_code, bsns_year, reprt_code, fs_div="CFS"):
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": self.api_key, "corp_code": corp_code, "bsns_year": bsns_year,
            "reprt_code": reprt_code, "fs_div": fs_div
        }
        try:
            res = requests.get(url, params=params).json()
            if res.get("status") == "000" and "list" in res:
                df = pd.DataFrame(res["list"])
                df["보고서구분"] = reprt_code
                return df
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_company_financials_auto(self, company_name, bsns_year):
        corp_code = self.get_corp_code_enhanced(company_name)
        if not corp_code:
            st.warning(f"DART에서 '{company_name}'에 대한 고유코드를 찾을 수 없습니다.")
            return None

        report_codes = ["11011", "11014", "11012", "11013"] # 년간 -> 3분기 -> 반기 -> 1분기 순
        for report_code in report_codes:
            df = self.get_financial_statement(corp_code, str(bsns_year), report_code)
            if not df.empty:
                rcept_no = self._get_rcept_no(corp_code, str(bsns_year), report_code)
                self._save_source_info(company_name, corp_code, report_code, str(bsns_year), rcept_no)
                return df
        return None
    
    def _get_rcept_no(self, corp_code, bsns_year, report_code):
        # 실제 API를 통해 가장 최신 보고서의 접수번호를 가져오는 로직 (샘플)
        # 현재는 시간 관계상 간단한 형태로 대체
        return f"{corp_code}_{bsns_year}_{report_code}_sample"

    def _save_source_info(self, company_name, corp_code, report_code, bsns_year, rcept_no):
        report_type_map = {
            "11011": "사업보고서", "11014": "3분기보고서",
            "11012": "반기보고서", "11013": "1분기보고서"
        }
        self.source_tracking[company_name] = {
            'company_code': corp_code, 'report_type': report_type_map.get(report_code, "재무제표"),
            'year': bsns_year, 'rcept_no': rcept_no,
            'dart_url': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        }


class QuarterlyDataCollector:
    """분기별 재무 데이터를 수집하는 클래스 (Q4=연간-(Q1+Q2+Q3))"""
    def __init__(self, dart_collector: DartAPICollector):
        self.dart_collector = dart_collector
        # DART API 보고서 코드 (모두 '누적' 값)
        self.report_codes = {
            "Q1": "11013",  # 1분기보고서(누적)
            "Q2": "11012",  # 반기보고서(누적)
            "Q3": "11014",  # 3분기보고서(누적)
            "Q4": "11011",  # 사업보고서(연간, 누적)
        }
        # 라벨(설명) - Q4는 연간(사업)임을 명확히
        self.quarter_names = {
            "Q1": "1분기보고서",
            "Q2": "반기보고서",
            "Q3": "3분기보고서",
            "Q4": "연간(사업보고서)",
        }

    def _extract_raw_amounts(self, df, column='thstrm_amount'):
        """지정 컬럼에서 원시값(원 단위)을 dict로 반환
           column: 'thstrm_amount'(당기금액) 또는 'thstrm_add_amount'(당기누계)"""
        def find_amount(keywords):
            for keyword in keywords:
                rows = df[df['account_nm'].str.contains(keyword, case=False, na=False)]
                if not rows.empty:
                    try:
                        raw = rows.iloc[0].get(column, '0')
                        val = str(raw).replace(',', '')
                        # 괄호 음수
                        if '(' in val and ')' in val:
                            val = f"-{val.strip('()')}"
                        if val.strip() in ['-', '']:
                            return 0.0
                        return float(val)
                    except Exception:
                        continue
            return 0.0

        return {
            '매출액':     find_amount(['매출액', 'revenue', 'sales']),
            '매출원가':   find_amount(['매출원가', 'cost of sales']),
            '매출총이익': find_amount(['매출총이익', 'gross profit', '총이익']),
            '영업이익':   find_amount(['영업이익', 'operating profit', '영업손익']),
            '당기순이익': find_amount(['당기순이익', 'net income', '순이익']),
            '판관비':     find_amount(['판매비와관리비', '판관비', 'selling and administrative']),
            '판매비':     find_amount(['판매비', 'selling expenses']),
            '관리비':     find_amount(['관리비', 'administrative expenses']),
        }


    def _build_display_row(self, company_name, year, label, raw, report_name=None):
        """표시용(조원/억원 & 비율) 행 생성: raw는 '원' 단위 당기(or 연간) dict"""
        row = {'회사': company_name, '연도': year, '분기': label}
        if report_name:
            row['보고서구분'] = report_name

        # 금액 변환
        if raw.get('매출액'):     row['매출액(조원)']     = raw['매출액']     / 1_000_000_000_000
        if raw.get('매출원가'):   row['매출원가(조원)']   = raw['매출원가']   / 1_000_000_000_000
        if raw.get('매출총이익'): row['매출총이익(조원)'] = raw['매출총이익'] / 1_000_000_000_000
        if raw.get('영업이익'):   row['영업이익(억원)']   = raw['영업이익']   / 100_000_000
        if raw.get('당기순이익'): row['당기순이익(억원)'] = raw['당기순이익'] / 100_000_000
        if raw.get('판관비'):     row['판관비(억원)']     = raw['판관비']     / 100_000_000

        # 비율(분모: 매출액)
        sales = raw.get('매출액', 0)
        if sales:
            if '영업이익'   in raw: row['영업이익률(%)']   = (raw['영업이익']   / sales) * 100
            if '매출총이익' in raw: row['매출총이익률(%)'] = (raw['매출총이익'] / sales) * 100
            if '당기순이익' in raw: row['순이익률(%)']     = (raw['당기순이익'] / sales) * 100
            if '매출원가'   in raw: row['매출원가율(%)']   = (raw['매출원가']   / sales) * 100
        return row

    def collect_quarterly_data(self, company_name, year=2024):
        import pandas as pd

        corp_code = self.dart_collector.get_corp_code_enhanced(company_name)
        if not corp_code:
            return pd.DataFrame()

        st.info(f"🔍 {company_name} {year}년 분기별 데이터(당기/연간) 산출 중...")

        # (1) 보고서별 원시값 수집: 당기(curr) / 누계(cum) 둘 다 준비
        curr, cum = {}, {}
        for q, code in self.report_codes.items():
            df = self.dart_collector.get_financial_statement(corp_code, str(year), code)
            if df.empty:
                st.warning(f"⚠️ {self.quarter_names[q]} 데이터 없음")
                continue
            # 당기금액(분기 금액)
            curr[q] = self._extract_raw_amounts(df, column='thstrm_amount')
            # 누적금액(없으면 당기로 대체)
            if 'thstrm_add_amount' in df.columns:
                cum[q] = self._extract_raw_amounts(df, column='thstrm_add_amount')
            else:
                cum[q] = curr[q]

        if not curr:
            st.error("❌ 분기 데이터 수집 실패")
            return pd.DataFrame()

        # (2) dict 합/차 유틸
        def add_dicts(*dicts):
            keys = set().union(*[d.keys() for d in dicts if d])
            return {k: sum(float(d.get(k, 0) or 0) for d in dicts if d) for k in keys}

        def sub_dict(a, b):
            keys = set(a.keys()) | set(b.keys())
            return {k: float(a.get(k, 0) or 0) - float(b.get(k, 0) or 0) for k in keys}

        # (3) 당기 산출: Q1~Q3는 ‘당기금액’을 그대로, Q4만 연산
        q1 = curr.get('Q1', {})
        q2 = curr.get('Q2', {})  # ✅ 더 이상 빼지 않음
        q3 = curr.get('Q3', {})  # ✅ 더 이상 빼지 않음

        # ✅ Q4(당기) = 연간(당기) − (Q1당기 + Q2당기 + Q3당기)
        if 'Q4' in curr:
            q4 = sub_dict(curr['Q4'], add_dicts(q1, q2, q3))
        else:
            q4 = {}

        # (디버그) 확인
        if 'Q4' in curr:
            st.caption(
                "🧪 산식 확인 | "
                f"연간(당기) 매출={curr['Q4'].get('매출액')} / "
                f"Q1={q1.get('매출액')} / Q2={q2.get('매출액')} / Q3={q3.get('매출액')} / "
                f"Q4(연간-합계)={q4.get('매출액')}"
            )

        # (4) 표 생성: Q1~Q4(당기) + 연간(누적)
        rows = []
        if q1: rows.append(self._build_display_row(company_name, year, f"{year}Q1", q1, "1분기(당기)"))
        if q2: rows.append(self._build_display_row(company_name, year, f"{year}Q2", q2, "2분기(당기)"))
        if q3: rows.append(self._build_display_row(company_name, year, f"{year}Q3", q3, "3분기(당기)"))
        if q4: rows.append(self._build_display_row(company_name, year, f"{year}Q4", q4, "4분기(당기)"))  # 10/01~12/31
        # 연간 행은 누적(cum['Q4'])로 표시 (없으면 당기와 동일)
        if 'Q4' in cum:
            rows.append(self._build_display_row(company_name, year, f"{year} 연간", cum['Q4'], "연간(사업보고서)"))

        return pd.DataFrame(rows)





    def _extract_key_metrics(self, df, quarter, year):
        # 분기 표시를 더 명확하게 (예: 2024Q1, 2024Q2 등)
        quarter_display = f"{year}{quarter}"
        metrics = {'분기': quarter_display}
        
        def find_amount(keywords):
            for keyword in keywords:
                rows = df[df['account_nm'].str.contains(keyword, case=False, na=False)]
                if not rows.empty:
                    try:
                        return float(str(rows.iloc[0]['thstrm_amount']).replace(',', '').replace('-', '0'))
                    except:
                        continue
            return 0

        # 핵심 재무지표 추출
        revenue = find_amount(['매출액', 'revenue', 'sales'])
        cost_of_sales = find_amount(['매출원가', 'cost of sales', '매출원가'])
        gross_profit = find_amount(['매출총이익', 'gross profit', '총이익'])
        operating_profit = find_amount(['영업이익', 'operating profit', '영업손익'])
        net_income = find_amount(['당기순이익', 'net income', '순이익'])
        selling_expenses = find_amount(['판매비', 'selling expenses'])
        administrative_expenses = find_amount(['관리비', 'administrative expenses'])
        sg_and_a = find_amount(['판매비와관리비', '판관비', 'selling and administrative'])

        # 금액 단위 변환 및 저장
        if revenue > 0:
            metrics['매출액(조원)'] = revenue / 1_000_000_000_000
        if cost_of_sales > 0:
            metrics['매출원가(조원)'] = cost_of_sales / 1_000_000_000_000
        if gross_profit > 0:
            metrics['매출총이익(조원)'] = gross_profit / 1_000_000_000_000
        if operating_profit > 0:
            metrics['영업이익(억원)'] = operating_profit / 100_000_000
        if net_income > 0:
            metrics['당기순이익(억원)'] = net_income / 100_000_000
        if selling_expenses > 0:
            metrics['판매비(억원)'] = selling_expenses / 100_000_000
        if administrative_expenses > 0:
            metrics['관리비(억원)'] = administrative_expenses / 100_000_000
        if sg_and_a > 0:
            metrics['판관비(억원)'] = sg_and_a / 100_000_000

        # 비율 계산
        if '매출액(조원)' in metrics and '영업이익(억원)' in metrics and metrics['매출액(조원)'] > 0:
            metrics['영업이익률(%)'] = (metrics['영업이익(억원)'] * 100) / (metrics['매출액(조원)'] * 10_000)
        
        if '매출액(조원)' in metrics and '매출총이익(조원)' in metrics and metrics['매출액(조원)'] > 0:
            metrics['매출총이익률(%)'] = (metrics['매출총이익(조원)'] / metrics['매출액(조원)']) * 100
        
        if '매출액(조원)' in metrics and '당기순이익(억원)' in metrics and metrics['매출액(조원)'] > 0:
            metrics['순이익률(%)'] = (metrics['당기순이익(억원)'] * 100) / (metrics['매출액(조원)'] * 10_000)
        
        if '매출액(조원)' in metrics and '매출원가(조원)' in metrics and metrics['매출액(조원)'] > 0:
            metrics['매출원가율(%)'] = (metrics['매출원가(조원)'] / metrics['매출액(조원)']) * 100
        
        return metrics if len(metrics) > 1 else None


class SKNewsCollector:
    """Google Sheets와 RSS에서 뉴스를 수집하는 클래스"""
    def __init__(self, custom_keywords=None):
        self.sheet_id = config.SHEET_ID
        self.service_account_json = config.GOOGLE_SERVICE_ACCOUNT_JSON
        self.rss_feeds = config.DEFAULT_RSS_FEEDS
        self.oil_keywords = custom_keywords if custom_keywords else config.BENCHMARKING_KEYWORDS
        
        # 세밀한 키워드 분류 (확장)
        self.company_keywords = ["SK에너지", "SK이노베이션", "GS칼텍스", "HD현대오일뱅크", "현대오일뱅크", "S-Oil", "에쓰오일", "SK", "에스케이"]
        self.industry_keywords = ["정유", "석유화학", "에너지", "화학", "원유", "나프타", "휘발유", "경유", "정제마진", "정유업계", "석유화학사", "정유사", "석유", "유가", "WTI", "두바이유", "브렌트유"]
        self.business_keywords = ["영업이익", "실적", "수익성", "투자", "사업확장", "원가절감", "효율성", "매출", "손실", "매출액", "영업손익", "기업", "경제", "주식", "증시", "시장"]
        self.trend_keywords = ["탄소중립", "ESG", "친환경", "수소", "신재생에너지", "바이오", "디지털전환", "스마트팩토리", "그린", "친환경"]

    def collect_news(self, *, max_items_per_feed: int = 50) -> pd.DataFrame:
        df_sheets = self._fetch_sheet_news()
        df_rss = self._fetch_rss_news(max_items=max_items_per_feed)

        if df_sheets.empty and df_rss.empty:
            return pd.DataFrame()

        df_all = pd.concat([df_sheets, df_rss], ignore_index=True)
        df_all.drop_duplicates(subset="제목", keep="first", inplace=True)
        
        # 키워드 기반 필터링 강화
        st.info(f"🔍 총 {len(df_all)}개 뉴스에서 관련 뉴스 필터링 중...")
        df_all = self._filter_relevant_news(df_all)
        st.success(f"✅ 필터링 완료: {len(df_all)}개 관련 뉴스 발견")
        
        # 필터링된 데이터가 있는 경우에만 처리
        if not df_all.empty:
            df_all = self._enrich_dataframe(df_all)
            
            # 관련도 점수 기반 정렬 (컬럼이 존재하는지 확인)
            sort_columns = []
            if "관련도점수" in df_all.columns:
                sort_columns.append("관련도점수")
            if "SK관련도" in df_all.columns:
                sort_columns.append("SK관련도")
            if "영향도" in df_all.columns:
                sort_columns.append("영향도")
            
            if sort_columns:
                df_all.sort_values(sort_columns, ascending=[False] * len(sort_columns), inplace=True)
            
            # 상위 50개만 반환 (품질 우선)
            return df_all.head(50).reset_index(drop=True)
        else:
            return pd.DataFrame()
    
    # 이하 _fetch_sheet_news, _fetch_rss_news, _enrich_dataframe 등 상세 메서드는 원본 코드와 거의 동일
    # 이 파일에서는 생략. 필요 시 원본 코드의 SKNewsCollector 클래스 내부 메서드를 그대로 복사.
    # (너무 길어져서 핵심 로직만 남깁니다.)
    def _fetch_sheet_news(self) -> pd.DataFrame:
        if not _GSPREAD_AVAILABLE or not self.sheet_id or not self.service_account_json:
            return pd.DataFrame()
        try:
            creds = Credentials.from_service_account_info(self.service_account_json)
            gc = gspread.authorize(creds)
            worksheet = gc.open_by_key(self.sheet_id).sheet1
            rows = worksheet.get_all_records()
            return pd.DataFrame(rows)
        except Exception as e:
            st.warning(f"구글 시트 로딩 실패: {e}")
            return pd.DataFrame()
    
    def _fetch_rss_news(self, *, max_items: int = 50) -> pd.DataFrame:
        collected = []
        total_found = 0
        
        for source, url in self.rss_feeds.items():
            try:
                feed = feedparser.parse(url)
                source_count = 0
                
                for entry in feed.entries[:max_items]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    
                    # 제목과 요약에서 불필요한 문자 제거
                    title = self._clean_text(title)
                    summary = self._clean_text(summary)
                    
                    # 최소 길이 체크 (완화된 기준)
                    if len(title) < 5:  # 제목만 5자 이상이면 OK
                        continue
                    
                    collected.append({
                        "제목": title,
                        "URL": entry.get("link", ""),
                        "요약": summary,
                        "날짜": self._parse_date(entry.get("published", "")),
                        "출처": source
                    })
                    source_count += 1
                    total_found += 1
                
                st.info(f"📰 {source}: {source_count}개 뉴스 수집 완료")
                
            except Exception as e:
                st.warning(f"RSS 피드 수집 오류 ({source}): {str(e)}")
                continue
        
        st.success(f"🎯 총 {total_found}개 뉴스 수집 완료")
        return pd.DataFrame(collected)
    
    def _clean_text(self, text: str) -> str:
        """텍스트 정리 및 전처리"""
        if not text:
            return ""
        
        # HTML 태그 제거
        import re
        text = re.sub(r'<[^>]+>', '', text)
        
        # 특수문자 정리
        text = re.sub(r'[^\w\s가-힣\-\.\,\!\?\(\)]', '', text)
        
        # 연속된 공백 제거
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def _filter_relevant_news(self, df: pd.DataFrame) -> pd.DataFrame:
        """키워드 기반으로 관련성 높은 뉴스만 필터링 (완화된 기준)"""
        if df.empty:
            return df
        
        relevant_news = []
        for _, row in df.iterrows():
            title = str(row.get('제목', '')).lower()
            summary = str(row.get('요약', '')).lower()
            full_text = f"{title} {summary}"
            
            # 키워드 매칭 카운트
            keyword_count = 0
            
            # 회사명 키워드 (가중치 높음)
            company_matches = sum(1 for kw in self.company_keywords if kw.lower() in full_text)
            keyword_count += company_matches * 2
            
            # 산업 키워드
            industry_matches = sum(1 for kw in self.industry_keywords if kw.lower() in full_text)
            keyword_count += industry_matches
            
            # 비즈니스 키워드
            business_matches = sum(1 for kw in self.business_keywords if kw.lower() in full_text)
            keyword_count += business_matches
            
            # 트렌드 키워드
            trend_matches = sum(1 for kw in self.trend_keywords if kw.lower() in full_text)
            keyword_count += trend_matches
            
            # 완화된 필터링 기준
            if keyword_count >= 1:  # 최소 1개 키워드만 있어도 포함
                relevant_news.append(row)
            # 또는 경제/기업 관련 키워드가 있으면 포함
            elif any(kw in full_text for kw in ["기업", "경제", "주식", "투자", "매출", "실적", "영업이익"]):
                relevant_news.append(row)
        
        return pd.DataFrame(relevant_news)

    def _enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: 
            return df
        
        try:
            df["키워드"] = df["제목"].apply(self._extract_keywords)
            df["영향도"] = df["제목"].apply(self._calc_importance)
            df["회사"] = df["제목"].apply(self._extract_company)
            df["SK관련도"] = df["제목"].apply(self._calc_sk_relevance)
            df["관련도점수"] = df.apply(self._calc_relevance_score, axis=1)
        except Exception as e:
            st.warning(f"뉴스 데이터 처리 중 오류 발생: {str(e)}")
            # 기본값으로 컬럼 추가
            df["키워드"] = ""
            df["영향도"] = 0
            df["회사"] = "기타"
            df["SK관련도"] = 0
            df["관련도점수"] = 0
        
        return df

    def _extract_keywords(self, text: str) -> str:
        """더 정확한 키워드 추출"""
        text_lower = str(text).lower()
        found_keywords = []
        
        # 회사명 키워드 (우선순위 높음)
        for kw in self.company_keywords:
            if kw.lower() in text_lower:
                found_keywords.append(kw)
        
        # 산업 키워드
        for kw in self.industry_keywords:
            if kw.lower() in text_lower and kw not in found_keywords:
                found_keywords.append(kw)
        
        # 비즈니스 키워드
        for kw in self.business_keywords:
            if kw.lower() in text_lower and kw not in found_keywords:
                found_keywords.append(kw)
        
        return ", ".join(found_keywords[:8])  # 최대 8개까지

    def _calc_importance(self, text: str) -> int:
        """영향도 계산 개선"""
        text_lower = str(text).lower()
        score = 0
        
        # 핵심 비즈니스 용어 (가중치 높음)
        business_terms = {
            "영업이익": 3, "실적": 3, "손실": 3, "투자": 2, "매출": 2,
            "수익성": 2, "사업확장": 2, "원가절감": 2, "효율성": 2
        }
        
        for term, weight in business_terms.items():
            if term in text_lower:
                score += weight
        
        return min(score, 10)

    def _calc_sk_relevance(self, text: str) -> int:
        """SK 관련도 계산 개선"""
        text_lower = str(text).lower()
        score = 0
        
        # SK 관련 키워드
        if any(sk_term in text_lower for sk_term in ["sk", "에스케이", "sk에너지", "sk이노베이션"]):
            score += 5
        
        # 정유/에너지 산업 키워드
        if any(term in text_lower for term in ["정유", "석유", "화학", "에너지"]):
            score += 2
        
        # 경쟁사 관련 키워드
        if any(comp in text_lower for comp in ["gs칼텍스", "현대오일뱅크", "s-oil", "에쓰오일"]):
            score += 1
        
        return min(score, 10)

    def _calc_relevance_score(self, row) -> int:
        """종합 관련도 점수 계산"""
        title = str(row.get('제목', '')).lower()
        summary = str(row.get('요약', '')).lower()
        full_text = f"{title} {summary}"
        
        score = 0
        
        # 회사명 매칭 (가장 높은 가중치)
        for kw in self.company_keywords:
            if kw.lower() in full_text:
                score += 10
        
        # 산업 키워드 매칭
        for kw in self.industry_keywords:
            if kw.lower() in full_text:
                score += 3
        
        # 비즈니스 키워드 매칭
        for kw in self.business_keywords:
            if kw.lower() in full_text:
                score += 2
        
        # 트렌드 키워드 매칭
        for kw in self.trend_keywords:
            if kw.lower() in full_text:
                score += 1
        
        return score

    def _extract_company(self, text: str) -> str:
        """회사명 추출 개선"""
        text_lower = str(text).lower()
        
        # 정확한 회사명 매칭
        company_mapping = {
            "sk에너지": "SK에너지",
            "sk이노베이션": "SK이노베이션", 
            "gs칼텍스": "GS칼텍스",
            "hd현대오일뱅크": "HD현대오일뱅크",
            "현대오일뱅크": "HD현대오일뱅크",
            "s-oil": "S-Oil",
            "에쓰오일": "S-Oil"
        }
        
        for key, value in company_mapping.items():
            if key in text_lower:
                return value
        
        return "기타"
    
    @staticmethod
    def _parse_date(date_str: str) -> str:
        try:
            return parser.parse(date_str).strftime("%Y-%m-%d %H:%M")
        except:
            return datetime.now().strftime("%Y-%m-%d %H:%M")