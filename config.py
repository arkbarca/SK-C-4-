# -*- coding: utf-8 -*-

# ==========================
# API 키 및 인증 정보
# Streamlit 배포 시에는 st.secrets를 사용하는 것이 안전합니다.
# 예: DART_API_KEY = st.secrets["DART_API_KEY"]
# ==========================
DART_API_KEY = "9a153f4344ad2db546d651090f78c8770bd773cb"
GEMINI_API_KEY = "AIzaSyB176ys4MCjEs8R0dv15hMqDE2G-9J0qIA" # 보안에 매우 유의하세요.

# 구글시트 설정
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/16g1G89xoxyqF32YLMD8wGYLnQzjq2F_ew6G1AHH4bCA/edit?usp=sharing"
SHEET_ID = "16g1G89xoxyqF32YLMD8wGYLnQzjq2F_ew6G1AHH4bCA"

# 구글 서비스 계정 키 (JSON 형식)
# 보안을 위해 st.secrets 또는 환경 변수로 관리하는 것을 강력히 권장합니다.
# 예: GOOGLE_SERVICE_ACCOUNT_JSON = st.secrets["gcp_service_account"]
GOOGLE_SERVICE_ACCOUNT_JSON = None # 여기에 JSON 내용을 직접 넣거나, st.secrets에서 불러오세요.

# ==========================
# 시각화 및 UI 설정
# ==========================
SK_COLORS = {
    'primary': '#E31E24',      # SK 레드
    'secondary': '#FF6B35',    # SK 오렌지
    'accent': '#004EA2',       # SK 블루
    'success': '#00A651',      # 성공 색상
    'warning': '#FF9500',      # 경고 색상
    'competitor': '#6C757D',   # 기본 경쟁사 색상 (회색)
    'competitor_1': '#AEC6CF', # 파스텔 블루
    'competitor_2': '#FFB6C1', # 파스텔 핑크
    'competitor_3': '#98FB98', # 파스텔 그린
    'competitor_4': '#F0E68C', # 파스텔 옐로우
    'competitor_5': '#DDA0DD', # 파스텔 퍼플
    'competitor_green': '#98FB98',   # 파스텔 그린
    'competitor_blue': '#AEC6CF',    # 파스텔 블루
    'competitor_yellow': '#F0E68C',  # 파스텔 옐로우
    'competitor_purple': '#DDA0DD',  # 파스텔 퍼플
    'competitor_orange': '#FFB347',  # 파스텔 오렌지
    'competitor_mint': '#98FF98',    # 파스텔 민트
}

# 분석 대상 회사 목록 (UI에서 사용)
COMPANIES_LIST = ["SK에너지", "GS칼텍스", "HD현대오일뱅크", "S-Oil"]
DEFAULT_SELECTED_COMPANIES = ["SK에너지", "GS칼텍스"]

# ==========================
# DART API 관련 설정
# ==========================
COMPANY_NAME_MAPPING = {
    "SK에너지": ["SK에너지", "SK에너지주식회사", "에스케이에너지", "SK ENERGY"],
    "GS칼텍스": ["GS칼텍스", "지에스칼텍스", "GS칼텍스주식회사"],
    "HD현대오일뱅크": ["HD현대오일뱅크", "HD현대오일뱅크주식회사", "현대오일뱅크", "현대오일뱅크주식회사", "HYUNDAI OILBANK", "267250"],
    "현대오일뱅크": ["HD현대오일뱅크", "HD현대오일뱅크주식회사", "현대오일뱅크", "현대오일뱅크주식회사"],
    "S-Oil": ["S-Oil", "S-Oil Corporation", "에쓰오일", "에스오일", "주식회사S-Oil", "S-OIL", "s-oil", "010950"]
}

STOCK_CODE_MAPPING = {
    "S-Oil": "010950",
    "GS칼텍스": "089590",
    "HD현대오일뱅크": "267250",
    "현대오일뱅크": "267250",
    "SK에너지": "096770",
}

# ==========================
# 뉴스 수집 관련 설정
# ==========================
DEFAULT_RSS_FEEDS = {
    "연합뉴스_경제":   "https://www.yna.co.kr/rss/economy.xml",
    "조선일보_경제":   "https://www.chosun.com/arc/outboundfeeds/rss/category/economy/",
    "한국경제":       "https://www.hankyung.com/feed/economy",
    "서울경제":       "https://www.sedaily.com/RSSFeed.xml",
    "매일경제":       "https://www.mk.co.kr/rss/30000001/",
    "이데일리":       "https://www.edaily.co.kr/rss/rss_economy.xml",
}

OIL_KEYWORDS = [
    "SK", "SK에너지", "SK이노베이션", "GS칼텍스", "HD현대오일뱅크", "현대오일뱅크", "S-Oil", "에쓰오일",
    "정유", "유가", "원유", "석유", "화학", "에너지", "나프타", "휘발유", "경유",
    "정제마진", "WTI", "두바이유", "브렌트유", "영업이익", "실적", "수익성", "투자",
    "탄소중립", "ESG", "친환경", "수소", "신재생에너지",
]

# 벤치마킹 키워드 (뉴스 분석용) - 세밀한 분류
BENCHMARKING_KEYWORDS = [
    # 핵심 회사명
    "SK에너지", "SK이노베이션", "GS칼텍스", "HD현대오일뱅크", "현대오일뱅크", "S-Oil", "에쓰오일",
    
    # 산업 키워드
    "정유업계", "석유화학", "화학산업", "에너지산업", "정유사", "석유화학사",
    "원유", "나프타", "휘발유", "경유", "정제마진", "WTI", "두바이유", "브렌트유",
    
    # 비즈니스 키워드
    "영업이익", "실적", "수익성", "매출", "손실", "투자", "사업확장", "원가절감", 
    "효율성", "생산성", "경쟁력", "시장점유율", "매출액", "영업손익",
    
    # 전략 키워드
    "친환경", "탄소중립", "ESG", "신재생에너지", "수소", "바이오", 
    "디지털전환", "스마트팩토리", "4차산업혁명", "그린에너지",
    
    # 시장 동향
    "유가", "석유가격", "화학제품", "석유제품", "에너지수요", "시장동향"
]


# ==========================
# 이메일 관련 설정
# ==========================
# 실제 발송 기능은 보안상 구현하지 않음. UI용 링크 목록만 제공.
MAIL_PROVIDERS = {
    "네이버": "https://mail.naver.com/",
    "구글(Gmail)": "https://mail.google.com/",
    "다음": "https://mail.daum.net/",
    "아웃룩(Outlook)": "https://outlook.live.com/",
}