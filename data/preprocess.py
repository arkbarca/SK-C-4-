# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import streamlit as st
import re
from bs4 import BeautifulSoup

class FinancialDataProcessor:
    """수동으로 업로드된 XBRL 파일을 처리하는 클래스"""
    # 회사별 재무제표 용어 매핑 (포괄적)
    COMPANY_TERM_MAPPING = {
        # SK에너지 용어 매핑
        'SK에너지': {
            '매출': ['매출액', '제품매출', '상품매출', '용역 및 기타매출'],
            '매출원가': ['매출원가', '제품매출원가', '상품매출원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비', '판매관리비'],
            '영업이익': ['영업이익', '영업손익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익', '순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익', 'EPS']
        },
        # HD현대오일뱅크 용어 매핑
        'HD현대오일뱅크': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익']
        },
        # S-Oil 용어 매핑
        'S-Oil': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익', '당기순이익(손실)'],
            '포괄손익': ['총기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익', '기본주당이익', '희석주당이익']
        },
        # GS칼텍스 용어 매핑
        'GS칼텍스': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익', '법인세차감전이익(손실)'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익']
        }
    }
    
    # 더 포괄적한 XBRL 태그 매핑 (정규식 패턴) - 피드백 기반 확장
    INCOME_STATEMENT_PATTERNS = {
        # 매출 관련 (더 광범위한 패턴)
        r'(revenue|sales|매출|수익|총매출|매출수익|operating.*revenue|매출액|제품매출|상품매출|용역.*매출)(?!.*cost|원가|비용)': '매출액',
        r'(cost.*revenue|cost.*sales|cost.*goods|매출원가|원가|판매원가|제품매출원가|매출원가|상품매출원가)': '매출원가',
        
        # 이익 관련 (피드백 우선순위)
        r'(operating.*income|operating.*profit|영업이익|영업손익|영업수익|영업이익|영업손익)(?!.*비용|expense)': '영업이익',
        r'(gross.*profit|총이익|매출총이익|총수익|매출총이익|총이익)': '매출총이익',
        r'(net.*income|net.*profit|당기순이익|순이익|당기.*순손익|net.*earnings|당기순이익|순손익)(?!.*loss)': '당기순이익',
        
        # 비용 관련 (피드백 우선순위)
        r'(selling.*expense|selling.*cost|판매비|판매비용|판매관련비용|판매비|판매관리비)': '판매비',
        r'(administrative.*expense|administrative.*cost|관리비|관리비용|일반관리비|관리비)': '관리비',
        r'(selling.*administrative|판매비.*관리비|판관비|판매.*관리.*비용|판매비와관리비|판관비|판매관리비)': '판관비',
        r'(employee.*benefit|employee.*cost|wage|salary|인건비|급여|임금|인건비|인사비)': '인건비',
        r'(depreciation|amortization|감가상각|상각비|감가상각비|감가상각비|감가상각)': '감가상각비',
        
        # 기타 항목 (피드백 기반)
        r'(interest.*expense|interest.*cost|이자비용|이자지급|이자비용|이자비)': '이자비용',
        r'(financial.*cost|금융비용|금융원가|금융비용|금융손실)': '금융비용',
        r'(non.*operating.*income|영업외수익|기타수익|영업외수익|기타영업외수익)': '영업외수익',
        r'(non.*operating.*expense|영업외비용|기타비용|영업외비용|기타영업외비용)': '영업외비용',
        
        # 추가 매핑 (피드백 기반)
        r'(법인세차감전이익|법인세차감전이익|법인세차감전순손익)': '법인세차감전이익',
        r'(법인세비용|법인세비용|법인세)': '법인세비용',
        r'(주당이익|EPS|주당이익|기본주당이익|희석주당이익)': '주당이익',
        r'(포괄손익|기타포괄손익|포괄손익|총포괄손익)': '포괄손익',
        
        # 추가 정확한 매핑 (회사별 특화)
        r'(매출총이익|총이익|매출총이익)': '매출총이익',
        r'(영업손익|영업이익|영업손익)': '영업이익',
        r'(당기순손익|당기순이익|순손익|순이익)': '당기순이익',
        r'(매출원가|원가|매출원가)': '매출원가',
        r'(판매비와관리비|판관비|판매관리비)': '판관비'
    }
    
    def __init__(self):
        self.company_data = {}
        # 정규식 미리 컴파일 (성능 향상)
        self.compiled_patterns = {}
        for pattern, item in self.INCOME_STATEMENT_PATTERNS.items():
            self.compiled_patterns[re.compile(pattern, re.IGNORECASE)] = item
    
    def _normalize_company_terms(self, data, company_name):
        """회사별 재무제표 용어를 표준화하는 메서드"""
        try:
            # 회사명 매칭 (부분 매칭 포함)
            matched_company = None
            for company_key in self.COMPANY_TERM_MAPPING.keys():
                if company_key in company_name or company_name in company_key:
                    matched_company = company_key
                    break
            
            if not matched_company:
                return data  # 매칭되는 회사가 없으면 원본 반환
            
            company_mapping = self.COMPANY_TERM_MAPPING[matched_company]
            normalized_data = {}
            
            for standard_term, company_terms in company_mapping.items():
                for company_term in company_terms:
                    if company_term in data:
                        normalized_data[standard_term] = data[company_term]
                        break
            
            return normalized_data
        except Exception as e:
            st.warning(f"⚠️ 용어 정규화 중 오류: {str(e)}")
            return data

    def load_file(self, uploaded_file):
        """개선된 XBRL 파일 로드 (속도 최적화 + 오류 처리 강화)"""
        try:
            # 파일 크기 체크 (50MB 제한)
            file_size = uploaded_file.size if hasattr(uploaded_file, 'size') else 0
            if file_size > 50 * 1024 * 1024:
                st.error(f"❌ 파일이 너무 큽니다 ({file_size/(1024*1024):.1f}MB). 50MB 이하로 업로드해주세요.")
                return None
            
            # 파일 처음부터 읽기
            uploaded_file.seek(0)
            content = uploaded_file.read()
            
            # 빠른 인코딩 감지 및 디코딩
            content_str = self._fast_decode(content)
            if not content_str:
                st.error("❌ 파일 인코딩을 읽을 수 없습니다.")
                return None
            
            # XML 파싱 (더 안전한 방식)
            try:
                # lxml이 있으면 사용, 없으면 기본 xml 파서 사용
                soup = BeautifulSoup(content_str, 'lxml-xml')
                if not soup.find():  # 파싱 실패 시 기본 파서 사용
                    soup = BeautifulSoup(content_str, 'xml')
            except Exception:
                soup = BeautifulSoup(content_str, 'html.parser')  # 최후 수단
            
            # 회사명 추출 (더 빠르고 정확하게)
            company_name = self._extract_company_name_fast(soup, uploaded_file.name)
            
            # 재무 데이터 추출 (최적화된 버전)
            financial_data = self._extract_financial_items_optimized(soup)
            
            if not financial_data:
                st.warning(f"⚠️ {uploaded_file.name}에서 재무 항목을 찾을 수 없습니다.")
                st.info("💡 파일이 표준 XBRL 형식인지 확인해주세요.")
                return None
            
            # 표준 손익계산서 구조로 변환
            income_statement = self._create_income_statement(financial_data, company_name)
            return income_statement
            
        except Exception as e:
            st.error(f"❌ 파일 처리 중 오류: {str(e)}")
            st.info("💡 파일 형식을 확인하고 다시 시도해주세요.")
            return None

    def _fast_decode(self, content):
        """최적화된 인코딩 감지 및 디코딩"""
        # 가장 일반적인 인코딩부터 시도 (한국어 환경 최적화)
        encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'iso-8859-1', 'ascii']
        
        for encoding in encodings:
            try:
                decoded = content.decode(encoding)
                # 한글이 제대로 디코딩되었는지 간단히 체크
                if '매출' in decoded or 'revenue' in decoded.lower():
                    return decoded
                return decoded  # 한글이 없어도 성공한 디코딩은 반환
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        # 모든 인코딩 실패 시 오류 무시하고 디코딩
        try:
            return content.decode('utf-8', errors='ignore')
        except:
            return None

    def _extract_company_name_fast(self, soup, filename):
        """최적화된 회사명 추출"""
        # 1단계: 표준 XBRL 태그에서 회사명 검색
        company_tags = [
            'EntityRegistrantName', 'CompanyName', 'entity', 'registrant',
            'ReportingEntityName', 'EntityName', 'CorporateName'
        ]
        
        for tag_name in company_tags:
            # 정확한 태그명으로 먼저 검색
            node = soup.find(tag_name)
            if node and node.string and len(node.string.strip()) > 1:
                return node.string.strip()
            
            # 부분 매칭으로 검색 (대소문자 무시)
            node = soup.find(lambda t: t.name and tag_name.lower() in t.name.lower())
            if node and node.string and len(node.string.strip()) > 1:
                return node.string.strip()
        
        # 2단계: 파일명에서 회사명 추출 (강화된 매핑)
        name = filename.split('.')[0].lower()
        name_mapping = {
            'sk': 'SK에너지',
            'skenergy': 'SK에너지',
            'gs': 'GS칼텍스',
            'gscaltex': 'GS칼텍스',
            'hd': 'HD현대오일뱅크',
            'hyundai': 'HD현대오일뱅크',
            'hdoil': 'HD현대오일뱅크',
            's-oil': 'S-Oil',
            'soil': 'S-Oil',
            'soilcorp': 'S-Oil'
        }
        
        for key, company in name_mapping.items():
            if key in name:
                return company
        
        # 3단계: 파일명 그대로 사용 (정리해서)
        clean_name = re.sub(r'[^A-Za-z가-힣0-9\s]', '', filename.split('.')[0])
        return clean_name if clean_name else "Unknown Company"

    def _extract_financial_items_optimized(self, soup):
        """최적화된 재무 항목 추출 (정확성 강화)"""
        items = {}
        processed_count = 0
        
        # 숫자가 포함된 태그만 사전 필터링 (성능 향상)
        numeric_tags = []
        for tag in soup.find_all():
            if tag.string and re.search(r'\d', tag.string):
                numeric_tags.append(tag)
        
        if not numeric_tags:
            st.warning("📊 숫자 데이터가 포함된 태그를 찾을 수 없습니다.")
            return items
        
        # 진행 상황 표시
        st.info(f"🔍 {len(numeric_tags)}개의 숫자 태그 발견, 분석 중...")
        
        # 각 태그 분석 (정확성 강화)
        for tag in numeric_tags:
            tag_text = tag.string.strip()
            
            # 숫자 추출 및 검증 (정확성 강화)
            try:
                # 괄호로 둘러싸인 음수 처리
                if '(' in tag_text and ')' in tag_text:
                    number_str = re.sub(r'[^\d.]', '', tag_text.replace('(', '').replace(')', ''))
                    if number_str:
                        value = -float(number_str)
                    else:
                        continue
                else:
                    # 일반적인 숫자 추출 (더 정확한 패턴)
                    number_str = re.sub(r'[^\d.-]', '', tag_text)
                    if number_str and number_str not in ['-', '.', '-.']:
                        value = float(number_str)
                    else:
                        continue
                
                # 너무 작은 값은 제외 (노이즈 제거) - 기준 상향 조정
                if abs(value) < 10000:  # 1만원 이상만 포함
                    continue
                    
            except (ValueError, TypeError):
                continue
            
            # 태그 정보 구성 (태그명 + 속성 + 부모 태그 정보)
            tag_info_parts = [tag.name.lower() if tag.name else '']
            if tag.attrs:
                tag_info_parts.extend([str(v).lower() for v in tag.attrs.values()])
            
            # 부모 태그 정보도 포함 (더 정확한 매칭을 위해)
            if tag.parent and tag.parent.name:
                tag_info_parts.append(tag.parent.name.lower())
            
            tag_info = ' '.join(tag_info_parts)
            
            # 정규식 패턴 매칭 (우선순위 기반)
            matched_item = None
            matched_priority = 0
            
            for pattern, standard_item in self.compiled_patterns.items():
                if pattern.search(tag_info):
                    # 우선순위 결정 (더 구체적인 매칭이 높은 우선순위)
                    current_priority = len(pattern.pattern)
                    if current_priority > matched_priority:
                        matched_item = standard_item
                        matched_priority = current_priority
            
            if matched_item:
                # 같은 항목이 이미 있으면 더 큰 절댓값으로 업데이트 (정확성 우선)
                if matched_item not in items or abs(value) > abs(items[matched_item]):
                    items[matched_item] = value
                processed_count += 1
        
        # 결과 검증 및 정리
        if items:
            # 중복 제거 및 정리
            cleaned_items = {}
            for key, value in items.items():
                if abs(value) > 0:  # 0이 아닌 값만 포함
                    cleaned_items[key] = value
            
            st.success(f"✅ {len(cleaned_items)}개 재무항목 추출 (총 {processed_count}개 태그 처리)")
            with st.expander("🔍 추출된 데이터 상세 보기"):
                for key, value in cleaned_items.items():
                    formatted_value = self._format_amount(value)
                    st.write(f"**{key}**: {formatted_value}")
            
            return cleaned_items
        else:
            st.warning("⚠️ 표준 재무 항목을 찾을 수 없습니다.")
        
        return items

    def _create_income_statement(self, data, company_name):
        """표준 손익계산서 구조 생성"""
        # 회사별 용어 정규화 적용
        normalized_data = self._normalize_company_terms(data, company_name)
        
        # 표준 손익계산서 항목 순서 (피드백 기반 구조별 분석)
        standard_items = [
            # 1. 매출액
            '매출액',
            # 2. 변동비 (매출원가)
            '매출원가',
            # 3. 공헌이익 (매출액 - 변동비)
            '공헌이익',
            # 4. 고정비
            '고정비', '판관비', '판매비', '관리비', '인건비', '감가상각비',
            # 5. 영업이익
            '영업이익', '매출총이익',
            # 6. 영업외손익
            '영업외손익', '영업외수익', '영업외비용', '금융비용', '이자비용',
            # 7. 당기순이익
            '당기순이익'
        ]
        
        # 파생 항목 계산 (누락된 항목 추정)
        calculated_items = self._calculate_derived_items(normalized_data)
        normalized_data.update(calculated_items)
        
        # 손익계산서 생성
        income_statement = []
        for item in standard_items:
            value = normalized_data.get(item, 0)
            if value != 0:  # 0이 아닌 값만 포함
                income_statement.append({
                    '구분': item,
                    company_name: self._format_amount(value),
                    f'{company_name}_원시값': value
                })
        
        # 비율 계산 및 추가
        ratios = self._calculate_ratios(normalized_data)
        for ratio_name, ratio_value in ratios.items():
            income_statement.append({
                '구분': ratio_name,
                company_name: f"{ratio_value:.2f}%",
                f'{company_name}_원시값': ratio_value
            })
        
        return pd.DataFrame(income_statement)

    def _calculate_derived_items(self, data):
        """파생 항목 계산 (피드백 기반 정확한 계산)"""
        calculated = {}
        
        # 1. 매출총이익 계산 (정확한 계산만)
        if '매출액' in data and '매출원가' in data:
            calculated['매출총이익'] = data['매출액'] - data['매출원가']
        
        # 2. 판관비 계산 (정확한 계산만)
        if '판매비' in data and '관리비' in data:
            calculated['판관비'] = data['판매비'] + data['관리비']
        
        # 3. 영업이익 계산 (정확한 계산만)
        if '매출총이익' in data and '판관비' in data and '영업이익' not in data:
            calculated['영업이익'] = data['매출총이익'] - data['판관비']
        
        # 4. 공헌이익 계산 (매출액 - 변동비)
        if '매출액' in data and '매출원가' in data:
            # 매출원가를 변동비로 가정 (정유업계 특성상 대부분 변동비)
            calculated['공헌이익'] = data['매출액'] - data['매출원가']
        
        # 5. 고정비 계산 (판관비 + 기타 고정비)
        if '판관비' in data:
            calculated['고정비'] = data['판관비']
        elif '판매비' in data and '관리비' in data:
            calculated['고정비'] = data['판매비'] + data['관리비']
        
        # 6. 영업외손익 계산
        if '영업외수익' in data and '영업외비용' in data:
            calculated['영업외손익'] = data['영업외수익'] - data['영업외비용']
        
        return calculated

    def _calculate_ratios(self, data):
        """핵심 재무비율 계산 (피드백 기반)"""
        ratios = {}
        매출액 = data.get('매출액', 0)
        
        if 매출액 <= 0:
            return ratios  # 매출액이 없으면 비율 계산 불가
        
        # 1. 핵심 수익성 비율 (피드백 우선순위)
        if '영업이익' in data:
            ratios['영업이익률(%)'] = round((data['영업이익'] / 매출액) * 100, 2)
        
        if '매출총이익' in data:
            ratios['매출총이익률(%)'] = round((data['매출총이익'] / 매출액) * 100, 2)
        
        if '당기순이익' in data:
            ratios['순이익률(%)'] = round((data['당기순이익'] / 매출액) * 100, 2)
        
        # 2. 추가 수익성 비율 (더 상세한 분석)
        if '법인세차감전이익' in data:
            ratios['법인세차감전이익률(%)'] = round((data['법인세차감전이익'] / 매출액) * 100, 2)
        
        if '영업외수익' in data:
            ratios['영업외수익률(%)'] = round((data['영업외수익'] / 매출액) * 100, 2)
        
        if '영업외비용' in data:
            ratios['영업외비용률(%)'] = round((data['영업외비용'] / 매출액) * 100, 2)
        
        # 3. 핵심 비용 비율 (피드백 우선순위)
        if '매출원가' in data:
            ratios['매출원가율(%)'] = round((data['매출원가'] / 매출액) * 100, 2)
        
        # 4. 고정비 비율 (금액 + 비율)
        if '고정비' in data:
            ratios['고정비율(%)'] = round((data['고정비'] / 매출액) * 100, 2)
        
        if '판관비' in data:
            ratios['판관비율(%)'] = round((data['판관비'] / 매출액) * 100, 2)
        
        # 5. 고정비 세분화 (인건비, 감가상각비)
        if '인건비' in data:
            ratios['인건비율(%)'] = round((data['인건비'] / 매출액) * 100, 2)
        
        if '감가상각비' in data:
            ratios['감가상각비율(%)'] = round((data['감가상각비'] / 매출액) * 100, 2)
        
        # 6. 공헌이익 관련 비율
        if '공헌이익' in data:
            ratios['공헌이익률(%)'] = round((data['공헌이익'] / 매출액) * 100, 2)
        
        # 7. 영업외손익 비율
        if '영업외손익' in data:
            ratios['영업외손익률(%)'] = round((data['영업외손익'] / 매출액) * 100, 2)
        
        # 8. 추가 효율성 비율
        if '영업이익' in data and '매출총이익' in data:
            ratios['영업이익/매출총이익(%)'] = round((data['영업이익'] / data['매출총이익']) * 100, 2)
        
        if '당기순이익' in data and '영업이익' in data:
            ratios['순이익/영업이익(%)'] = round((data['당기순이익'] / data['영업이익']) * 100, 2)
        
        return ratios

    def _format_amount(self, amount):
        """금액 포맷팅 (한국 단위 사용)"""
        if amount == 0:
            return "0원"
            
        abs_amount = abs(amount)
        sign = "▼ " if amount < 0 else ""
        
        if abs_amount >= 1_000_000_000_000:  # 1조 이상
            return f"{sign}{amount/1_000_000_000_000:.1f}조원"
        elif abs_amount >= 100_000_000:  # 1억 이상
            return f"{sign}{amount/100_000_000:.0f}억원"
        elif abs_amount >= 10_000:  # 1만 이상
            return f"{sign}{amount/10_000:.0f}만원"
        else:
            return f"{sign}{amount:,.0f}원"

    def merge_company_data(self, dataframes):
        """여러 회사 데이터 병합 (안전한 병합)"""
        if not dataframes:
            return pd.DataFrame()
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        # 기준이 되는 첫 번째 데이터프레임
        merged = dataframes[0].copy()
        
        # 나머지 데이터프레임들을 순차적으로 병합
        for df in dataframes[1:]:
            try:
                # 회사 컬럼만 추출 (구분, _원시값 컬럼 제외)
                company_cols = [col for col in df.columns 
                              if col != '구분' and not col.endswith('_원시값')]
                
                for company_col in company_cols:
                    # 구분을 인덱스로 하여 데이터 병합
                    company_data = df.set_index('구분')[company_col]
                    merged_temp = merged.set_index('구분')
                    merged_temp = merged_temp.join(company_data, how='outer')
                    merged = merged_temp.reset_index()
            except Exception as e:
                st.warning(f"⚠️ 데이터 병합 중 오류: {e}")
                continue
        
        # 결측치를 "-"로 채움
        merged = merged.fillna("-")
        
        return merged


class SKFinancialDataProcessor:
    """DART API 데이터를 SK에너지 중심의 손익계산서로 가공하는 클래스"""
    # 회사별 재무제표 용어 매핑 (DART API용)
    DART_COMPANY_TERM_MAPPING = {
        'SK에너지': {
            '매출': ['매출액', '제품매출', '상품매출', '용역 및 기타매출'],
            '매출원가': ['매출원가', '제품매출원가', '상품매출원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비', '판매관리비'],
            '영업이익': ['영업이익', '영업손익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익', '순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익', 'EPS']
        },
        'HD현대오일뱅크': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익']
        },
        'S-Oil': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익', '당기순이익(손실)'],
            '포괄손익': ['총기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익', '기본주당이익', '희석주당이익']
        },
        'GS칼텍스': {
            '매출': ['매출액', '매출'],
            '매출원가': ['매출원가', '원가'],
            '매출총이익': ['매출총이익', '총이익'],
            '판관비': ['판매비와관리비', '판매비', '관리비'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '영업외수익': ['금융수익', '기타수익'],
            '영업외비용': ['금융비용', '기타비용'],
            '법인세차감전이익': ['법인세차감전이익', '법인세차감전이익(손실)'],
            '법인세비용': ['법인세비용'],
            '당기순이익': ['당기순이익'],
            '포괄손익': ['기타포괄손익', '포괄손익'],
            '주당이익': ['주당이익']
        }
    }
    
    INCOME_STATEMENT_MAP = {
        # 매출 관련 (피드백 우선순위)
        'sales': '매출액', 'revenue': '매출액', '매출액': '매출액', '매출': '매출액', '제품매출': '매출액', '상품매출': '매출액',
        'costofgoodssold': '매출원가', 'cogs': '매출원가', '매출원가': '매출원가', '원가': '매출원가', '상품매출원가': '매출원가',
        
        # 이익 관련 (피드백 우선순위)
        'operatingincome': '영업이익', '영업이익': '영업이익', '영업손익': '영업이익', '영업손익': '영업이익',
        'grossprofit': '매출총이익', '매출총이익': '매출총이익', '총이익': '매출총이익', '총이익': '매출총이익',
        'netincome': '당기순이익', '당기순이익': '당기순이익', '순이익': '당기순이익', '순손익': '당기순이익',
        
        # 비용 관련 (피드백 우선순위)
        'operatingexpenses': '판관비', '판매비와관리비': '판관비', '판관비': '판관비', '판매관리비': '판관비',
        'sellingexpense': '판매비', '판매비': '판매비', '판매관리비': '판매비',
        'administrativeexpense': '관리비', '관리비': '관리비', '일반관리비': '관리비',
        'employeebenefit': '인건비', '인건비': '인건비', '인사비': '인건비',
        'depreciation': '감가상각비', '감가상각비': '감가상각비', '감가상각': '감가상각비',
        
        # 기타 항목 (피드백 기반)
        'interestexpense': '이자비용', '이자비용': '이자비용', '이자비': '이자비용',
        'financialcost': '금융비용', '금융비용': '금융비용', '금융손실': '금융비용',
        'nonoperatingincome': '영업외수익', '영업외수익': '영업외수익', '기타수익': '영업외수익', '기타영업외수익': '영업외수익',
        'nonoperatingexpense': '영업외비용', '영업외비용': '영업외비용', '기타비용': '영업외비용', '기타영업외비용': '영업외비용',
        'incometax': '법인세비용', '법인세비용': '법인세비용', '법인세': '법인세비용',
        'earningspershare': '주당이익', '주당이익': '주당이익', 'eps': '주당이익', '기본주당이익': '주당이익',
        'comprehensiveincome': '포괄손익', '포괄손익': '포괄손익', '기타포괄손익': '포괄손익', '총포괄손익': '포괄손익',
        
        # 추가 정확한 매핑 (회사별 특화)
        '매출총이익': '매출총이익', '총이익': '매출총이익',
        '영업손익': '영업이익', '영업이익': '영업이익',
        '당기순손익': '당기순이익', '순손익': '당기순이익',
        '매출원가': '매출원가', '원가': '매출원가',
        '판매비와관리비': '판관비', '판관비': '판관비'
    }

    def _normalize_dart_company_terms(self, data, company_name):
        """DART API용 회사별 재무제표 용어를 표준화하는 메서드"""
        try:
            # 회사명 매칭 (부분 매칭 포함)
            matched_company = None
            for company_key in self.DART_COMPANY_TERM_MAPPING.keys():
                if company_key in company_name or company_name in company_key:
                    matched_company = company_key
                    break
            
            if not matched_company:
                return data  # 매칭되는 회사가 없으면 원본 반환
            
            company_mapping = self.DART_COMPANY_TERM_MAPPING[matched_company]
            normalized_data = {}
            
            for standard_term, company_terms in company_mapping.items():
                for company_term in company_terms:
                    if company_term in data:
                        normalized_data[standard_term] = data[company_term]
                        break
            
            return normalized_data
        except Exception as e:
            st.warning(f"⚠️ DART 용어 정규화 중 오류: {str(e)}")
            return data

    def process_dart_data(self, dart_df, company_name):
        if dart_df.empty: return None
        financial_data = {}
        
        # 더 정확한 매핑을 위한 개선된 처리
        for _, row in dart_df.iterrows():
            account_nm = row.get('account_nm', '').strip()
            thstrm_amount = row.get('thstrm_amount', '0')
            
            try:
                val_str = str(thstrm_amount).replace(',', '')
                val = float(f"-{val_str.strip('()')}" if '(' in val_str else val_str)
            except:
                continue
            
            # 더 정확한 매핑 (우선순위 기반)
            matched_item = None
            matched_priority = 0
            
            for key, mapped_name in self.INCOME_STATEMENT_MAP.items():
                # 정확한 매칭 우선
                if key.lower() == account_nm.lower():
                    current_priority = 100  # 최고 우선순위
                elif key.lower() in account_nm.lower():
                    current_priority = len(key)  # 길이가 긴 매칭이 우선
                else:
                    continue
                
                if current_priority > matched_priority:
                    matched_item = mapped_name
                    matched_priority = current_priority
            
            if matched_item:
                # 같은 항목이 이미 있으면 더 큰 절댓값으로 업데이트
                if matched_item not in financial_data or abs(val) > abs(financial_data[matched_item]):
                    financial_data[matched_item] = val
        
        # 회사별 용어 정규화 적용
        normalized_data = self._normalize_dart_company_terms(financial_data, company_name)
        
        return self._create_income_statement(normalized_data, company_name)

    def _create_income_statement(self, data, company_name):
        """DART API용 표준 손익계산서 생성 (피드백 기반)"""
        # 파생 항목 계산
        calculated_items = self._calculate_derived_items(data)
        data.update(calculated_items)
        
        # 표준 손익계산서 항목 순서 (피드백 기반)
        standard_items = [
            '매출액', '매출원가', '공헌이익', '고정비', '판관비', 
            '영업이익', '매출총이익', '영업외손익', '당기순이익'
        ]
        
        # 손익계산서 생성
        statement = []
        for item in standard_items:
            if item in data and data[item] != 0:
                statement.append({
                    '구분': item,
                    company_name: f"{data[item]/100_000_000:,.0f}억원",
                    f'{company_name}_원시값': data[item]
                })
        
        # 비율 계산 및 추가
        ratios = self._calculate_ratios(data)
        for ratio_name, ratio_value in ratios.items():
            statement.append({
                '구분': ratio_name,
                company_name: f"{ratio_value:.2f}%",
                f'{company_name}_원시값': ratio_value
            })
        
        return pd.DataFrame(statement)
    
    def _calculate_derived_items(self, data):
        """DART API용 파생 항목 계산 (피드백 기반 정확한 계산)"""
        calculated = {}
        
        # 1. 매출총이익 계산 (정확한 계산만)
        if '매출액' in data and '매출원가' in data:
            calculated['매출총이익'] = data['매출액'] - data['매출원가']
        
        # 2. 판관비 계산 (정확한 계산만)
        if '판매비' in data and '관리비' in data:
            calculated['판관비'] = data['판매비'] + data['관리비']
        
        # 3. 영업이익 계산 (정확한 계산만)
        if '매출총이익' in data and '판관비' in data and '영업이익' not in data:
            calculated['영업이익'] = data['매출총이익'] - data['판관비']
        
        # 4. 공헌이익 계산 (매출액 - 변동비)
        if '매출액' in data and '매출원가' in data:
            # 매출원가를 변동비로 가정 (정유업계 특성상 대부분 변동비)
            calculated['공헌이익'] = data['매출액'] - data['매출원가']
        
        # 5. 고정비 계산 (판관비 + 기타 고정비)
        if '판관비' in data:
            calculated['고정비'] = data['판관비']
        elif '판매비' in data and '관리비' in data:
            calculated['고정비'] = data['판매비'] + data['관리비']
        
        # 6. 영업외손익 계산
        if '영업외수익' in data and '영업외비용' in data:
            calculated['영업외손익'] = data['영업외수익'] - data['영업외비용']
        
        return calculated
    
    def _calculate_ratios(self, data):
        """DART API용 핵심 재무비율 계산 (피드백 기반)"""
        ratios = {}
        매출액 = data.get('매출액', 0)
        
        if 매출액 <= 0:
            return ratios  # 매출액이 없으면 비율 계산 불가
        
        # 1. 핵심 수익성 비율 (피드백 우선순위)
        if '영업이익' in data:
            ratios['영업이익률(%)'] = round((data['영업이익'] / 매출액) * 100, 2)
        
        if '매출총이익' in data:
            ratios['매출총이익률(%)'] = round((data['매출총이익'] / 매출액) * 100, 2)
        
        if '당기순이익' in data:
            ratios['순이익률(%)'] = round((data['당기순이익'] / 매출액) * 100, 2)
        
        # 2. 추가 수익성 비율 (더 상세한 분석)
        if '법인세차감전이익' in data:
            ratios['법인세차감전이익률(%)'] = round((data['법인세차감전이익'] / 매출액) * 100, 2)
        
        if '영업외수익' in data:
            ratios['영업외수익률(%)'] = round((data['영업외수익'] / 매출액) * 100, 2)
        
        if '영업외비용' in data:
            ratios['영업외비용률(%)'] = round((data['영업외비용'] / 매출액) * 100, 2)
        
        # 3. 핵심 비용 비율 (피드백 우선순위)
        if '매출원가' in data:
            ratios['매출원가율(%)'] = round((data['매출원가'] / 매출액) * 100, 2)
        
        # 4. 고정비 비율 (금액 + 비율)
        if '고정비' in data:
            ratios['고정비율(%)'] = round((data['고정비'] / 매출액) * 100, 2)
        
        if '판관비' in data:
            ratios['판관비율(%)'] = round((data['판관비'] / 매출액) * 100, 2)
        
        # 5. 고정비 세분화 (인건비, 감가상각비)
        if '인건비' in data:
            ratios['인건비율(%)'] = round((data['인건비'] / 매출액) * 100, 2)
        
        if '감가상각비' in data:
            ratios['감가상각비율(%)'] = round((data['감가상각비'] / 매출액) * 100, 2)
        
        # 6. 공헌이익 관련 비율
        if '공헌이익' in data:
            ratios['공헌이익률(%)'] = round((data['공헌이익'] / 매출액) * 100, 2)
        
        # 7. 영업외손익 비율
        if '영업외손익' in data:
            ratios['영업외손익률(%)'] = round((data['영업외손익'] / 매출액) * 100, 2)
        
        # 8. 추가 효율성 비율
        if '영업이익' in data and '매출총이익' in data:
            ratios['영업이익/매출총이익(%)'] = round((data['영업이익'] / data['매출총이익']) * 100, 2)
        
        if '당기순이익' in data and '영업이익' in data:
            ratios['순이익/영업이익(%)'] = round((data['당기순이익'] / data['영업이익']) * 100, 2)
        
        return ratios


    def merge_company_data(self, dataframes: list[pd.DataFrame]):
        if not dataframes: return pd.DataFrame()
        if len(dataframes) == 1: return dataframes[0]

        merged_df = dataframes[0]
        for right_df in dataframes[1:]:
            merged_df = pd.merge(merged_df, right_df, on='구분', how='outer')
        
        # 컬럼 순서 재정렬 (SK에너지 우선)
        cols = merged_df.columns.tolist()
        sk_cols = [c for c in cols if 'SK에너지' in c]
        other_cols = [c for c in cols if c not in sk_cols and c != '구분']
        final_cols = ['구분'] + sk_cols + other_cols
        
        return merged_df[final_cols].fillna("-")