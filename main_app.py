# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from datetime import datetime

import config
from data.loader import DartAPICollector, SKNewsCollector, QuarterlyDataCollector

from data.preprocess import SKFinancialDataProcessor, FinancialDataProcessor 
from insight.gemini_api import GeminiInsightGenerator
from visualization.charts import (
    create_sk_bar_chart, create_sk_radar_chart, 
    create_quarterly_trend_chart, create_gap_trend_chart, 
    create_gap_analysis, create_gap_chart, PLOTLY_AVAILABLE
)
from util.export import create_excel_report, create_enhanced_pdf_report

st.set_page_config(page_title="SK에너지 경쟁사 분석 대시보드", page_icon="⚡", layout="wide")

def initialize_session_state():
    session_vars = [
        'financial_data', 'quarterly_data', 'news_data', 
        'financial_insight', 'news_insight', 'selected_companies', 
        'manual_financial_data', 'integrated_insight'
    ]
    for var in session_vars:
        if var not in st.session_state:
            st.session_state[var] = None
    if 'custom_keywords' not in st.session_state:
        st.session_state.custom_keywords = config.BENCHMARKING_KEYWORDS

def main():
    initialize_session_state()
    st.title("⚡ SK에너지 경쟁사 분석 대시보드")
    
    tabs = st.tabs(["📈 재무분석", "📁 수동 파일 업로드", "📰 뉴스분석", "🧠 통합 인사이트", "📄 보고서 생성"])
    
    with tabs[0]: # 재무분석 탭
        st.subheader("📈 DART 공시 데이터 심층 분석")
        selected_companies = st.multiselect("분석할 기업 선택", config.COMPANIES_LIST, default=config.DEFAULT_SELECTED_COMPANIES)
        analysis_year = st.selectbox("분석 연도", ["2024", "2023", "2022"])
        
        # 분기별 데이터 수집 옵션 추가
        st.markdown("---")
        st.subheader("📊 분기별 데이터 수집 설정")
        
        collect_quarterly = st.checkbox("📊 분기별 데이터 수집", value=True, help="1분기보고서, 반기보고서, 3분기보고서, 사업보고서를 모두 수집합니다")
        
        if collect_quarterly:
            quarterly_years = st.multiselect("분기별 분석 연도", ["2024", "2023", "2022"], default=["2024"], help="분기별 데이터를 수집할 연도를 선택하세요")
            st.info("📋 수집할 보고서: 1분기보고서(Q1, 누적) • 반기보고서(Q2, 누적) • 3분기보고서(Q3, 누적) • 사업보고서(연간, 누적)\n"
        "🔎 Q4(4분기 당기)는 연간 − (Q1+Q2+Q3)로 산출됩니다.")



        if st.button("🚀 DART 자동 분석 시작", type="primary"):
            with st.spinner("모든 데이터를 수집하고 심층 분석 중입니다..."):
                dart = DartAPICollector(config.DART_API_KEY)
                processor = SKFinancialDataProcessor()
                dataframes = [processor.process_dart_data(dart.get_company_financials_auto(c, analysis_year), c) for c in selected_companies]
                dataframes = [df for df in dataframes if df is not None]

                # 분기별 데이터 수집 (개선된 버전)
                q_data_list = []
                if collect_quarterly and quarterly_years:
                
                    q_collector = QuarterlyDataCollector(dart)

                    # (디버그) 실제 로드된 클래스/매핑 확인
                    st.caption(f"🧭 QuarterlyDataCollector 모듈 = {q_collector.__class__.__module__}")
                    st.caption(f"🧪 보고서코드 매핑 = {getattr(q_collector, 'report_codes', {})}")

                    
                    st.info(f"📊 분기별 데이터 수집 시작... ({', '.join(quarterly_years)}년, {len(selected_companies)}개 회사)")
                    
                    total_quarters = 0
                    for year in quarterly_years:
                        for company in selected_companies:
                            q_df = q_collector.collect_quarterly_data(company, int(year))
                            if not q_df.empty:
                                q_data_list.append(q_df)
                                total_quarters += len(q_df)
                    
                    if q_data_list:
                        st.success(f"✅ 분기별 데이터 수집 완료! 총 {len(q_data_list)}개 회사, {total_quarters}개 분기 데이터")
                    else:
                        st.warning("⚠️ 수집된 분기별 데이터가 없습니다.")

                if dataframes:
                    st.session_state.financial_data = processor.merge_company_data(dataframes)
                    if q_data_list:
                        st.session_state.quarterly_data = pd.concat(q_data_list, ignore_index=True)
                        st.success(f"✅ 총 {len(q_data_list)}개 회사의 분기별 데이터 수집 완료")
                    gemini = GeminiInsightGenerator(config.GEMINI_API_KEY)
                    st.session_state.financial_insight = gemini.generate_financial_insight(st.session_state.financial_data)
                else:
                    st.error("데이터 수집에 실패했습니다.")



        if 'financial_data' in st.session_state and st.session_state.financial_data is not None:
            st.markdown("---")
            st.subheader("💰 재무분석 결과")
            final_df = st.session_state.financial_data
            
            # 표시용 컬럼만 표시 (원시값 제외)
            display_cols = [col for col in final_df.columns if not col.endswith('_원시값')]
            st.markdown("**📋 정리된 재무지표 (표시값)**")
            st.dataframe(final_df[display_cols].set_index('구분'), use_container_width=True)

            st.markdown("---")
            st.subheader("📊 주요 지표 비교")
            ratio_df = final_df[final_df['구분'].str.contains('%', na=False)]
            raw_cols = [col for col in final_df.columns if col.endswith('_원시값')]
            if not ratio_df.empty and raw_cols:
                chart_df = pd.melt(ratio_df, id_vars=['구분'], value_vars=raw_cols, var_name='회사', value_name='수치')
                chart_df['회사'] = chart_df['회사'].str.replace('_원시값', '')
                if PLOTLY_AVAILABLE:
                    st.plotly_chart(create_sk_bar_chart(chart_df), use_container_width=True, key="dart_bar_chart")
                    st.plotly_chart(create_sk_radar_chart(chart_df), use_container_width=True, key="dart_radar_chart")

        if 'quarterly_data' in st.session_state and st.session_state.quarterly_data is not None:
            st.markdown("---")
            st.subheader("📈 분기별 성과 및 갭 추이 분석")
            
            # 분기별 데이터 요약 정보 표시
            quarterly_df = st.session_state.quarterly_data
            st.info(f"📊 수집된 분기별 데이터: {len(quarterly_df)}개 데이터포인트")
            
            # 분기별 데이터 요약 통계
            if '보고서구분' in quarterly_df.columns:
                report_summary = quarterly_df['보고서구분'].value_counts()
                st.markdown("**📋 수집된 보고서별 데이터 현황**")
                for report_type, count in report_summary.items():
                    st.write(f"• {report_type}: {count}개")
            
            # 분기별 데이터 테이블 표시
            st.markdown("**📋 분기별 재무지표 상세 데이터**")
            # '연간' 행 제거
            quarterly_df = quarterly_df[~quarterly_df["분기"].str.contains("연간")]
            st.dataframe(quarterly_df, use_container_width=True)
            
            if PLOTLY_AVAILABLE:
                # ✅ 분기가 '연간'이 아닌 행만 차트에 사용
                chart_input = quarterly_df.copy()
                if '분기' in chart_input.columns:
                   chart_input = chart_input[~chart_input['분기'].astype(str).str.contains('연간')]

                st.plotly_chart(create_quarterly_trend_chart(chart_input), use_container_width=True, key="dart_quarterly_trend")
                st.plotly_chart(create_gap_trend_chart(chart_input), use_container_width=True, key="dart_gap_trend")


        # 갭차이 분석 추가 (완전한 버전)
        if 'financial_data' in st.session_state and st.session_state.financial_data is not None:
            st.markdown("---")
            st.subheader("📈 갭차이 분석")
            final_df = st.session_state.financial_data
            raw_cols = [col for col in final_df.columns if col.endswith('_원시값')]
            if raw_cols and len(raw_cols) > 1:
                gap_analysis = create_gap_analysis(final_df, raw_cols)
                if not gap_analysis.empty:
                    st.markdown("**📊 SK에너지 대비 경쟁사 갭차이 분석표**")
                    st.dataframe(gap_analysis, use_container_width=True)
                    
                    # 갭차이 시각화
                    if PLOTLY_AVAILABLE:
                        st.markdown("**📈 갭차이 시각화 차트**")
                        st.plotly_chart(create_gap_chart(gap_analysis), use_container_width=True, key="dart_gap_chart")
                else:
                    st.warning("⚠️ 갭차이 분석을 위한 충분한 데이터가 없습니다. (최소 2개 회사 필요)")
            else:
                st.info("ℹ️ 갭차이 분석을 위해서는 최소 2개 이상의 회사 데이터가 필요합니다.")

        if 'financial_insight' in st.session_state and st.session_state.financial_insight:
            st.subheader("🤖 AI 재무 인사이트")
            st.markdown(st.session_state.financial_insight)
            
    with tabs[1]: # 수동 파일 업로드 탭
        st.subheader("📁 수동 XBRL 파일 업로드")
        st.info("💡 DART에서 다운로드한 XBRL 파일을 직접 업로드하여 분석할 수 있습니다.")
        
        uploaded_files = st.file_uploader(
            "XBRL 파일 선택 (여러 파일 업로드 가능)",
            type=['xml', 'xbrl', 'zip'],
            accept_multiple_files=True,
            help="DART에서 다운로드한 XBRL 파일을 업로드하세요. 여러 회사의 파일을 동시에 업로드할 수 있습니다."
        )
        
        if uploaded_files:
            if st.button("📊 수동 업로드 분석 시작", type="secondary"):
                with st.spinner("XBRL 파일을 분석하고 처리 중입니다..."):
                    processor = FinancialDataProcessor()
                    dataframes = []
                    
                    for uploaded_file in uploaded_files:
                        st.write(f"🔍 {uploaded_file.name} 처리 중...")
                        df = processor.load_file(uploaded_file)
                        if df is not None and not df.empty:
                            dataframes.append(df)
                            st.success(f"✅ {uploaded_file.name} 처리 완료")
                        else:
                            st.error(f"❌ {uploaded_file.name} 처리 실패")
                    
                    if dataframes:
                        st.session_state.manual_financial_data = processor.merge_company_data(dataframes)
                        st.session_state.financial_data = st.session_state.manual_financial_data
                        
                        # AI 인사이트 생성
                        gemini = GeminiInsightGenerator(config.GEMINI_API_KEY)
                        st.session_state.financial_insight = gemini.generate_financial_insight(st.session_state.manual_financial_data)
                        
                        st.success("✅ 수동 업로드 분석이 완료되었습니다!")
                    else:
                        st.error("❌ 처리할 수 있는 데이터가 없습니다.")

        # 수동 업로드 결과 표시 (재무분석 탭과 동일한 구조)
        if 'manual_financial_data' in st.session_state and st.session_state.manual_financial_data is not None:
            st.markdown("---")
            st.subheader("💰 수동 업로드 재무분석 결과")
            final_df = st.session_state.manual_financial_data
            
            # 표시용 컬럼만 표시 (원시값 제외)
            display_cols = [col for col in final_df.columns if not col.endswith('_원시값')]
            st.markdown("**📋 정리된 재무지표 (표시값)**")
            st.dataframe(final_df[display_cols].set_index('구분'), use_container_width=True)

            st.markdown("---")
            st.subheader("📊 주요 지표 비교")
            ratio_df = final_df[final_df['구분'].str.contains('%', na=False)]
            raw_cols = [col for col in final_df.columns if col.endswith('_원시값')]
            if not ratio_df.empty and raw_cols:
                chart_df = pd.melt(ratio_df, id_vars=['구분'], value_vars=raw_cols, var_name='회사', value_name='수치')
                chart_df['회사'] = chart_df['회사'].str.replace('_원시값', '')
                if PLOTLY_AVAILABLE:
                    st.plotly_chart(create_sk_bar_chart(chart_df), use_container_width=True, key="manual_bar_chart")
                    st.plotly_chart(create_sk_radar_chart(chart_df), use_container_width=True, key="manual_radar_chart")

            # 갭차이 분석 추가 (완전한 버전)
            st.markdown("---")
            st.subheader("📈 갭차이 분석")
            raw_cols = [col for col in final_df.columns if col.endswith('_원시값')]
            if raw_cols and len(raw_cols) > 1:
                gap_analysis = create_gap_analysis(final_df, raw_cols)
                if not gap_analysis.empty:
                    st.markdown("**📊 SK에너지 대비 경쟁사 갭차이 분석표**")
                    st.dataframe(gap_analysis, use_container_width=True)
                    
                    # 갭차이 시각화
                    if PLOTLY_AVAILABLE:
                        st.markdown("**📈 갭차이 시각화 차트**")
                        st.plotly_chart(create_gap_chart(gap_analysis), use_container_width=True, key="manual_gap_chart")
                else:
                    st.warning("⚠️ 갭차이 분석을 위한 충분한 데이터가 없습니다. (최소 2개 회사 필요)")
            else:
                st.info("ℹ️ 갭차이 분석을 위해서는 최소 2개 이상의 회사 데이터가 필요합니다.")

        if 'financial_insight' in st.session_state and st.session_state.financial_insight:
            st.subheader("🤖 AI 재무 인사이트")
            st.markdown(st.session_state.financial_insight)

    with tabs[2]: # 뉴스분석 탭
        st.subheader("📰 경쟁사 벤치마킹 뉴스 분석")
        st.sidebar.subheader("🔍 뉴스 검색 키워드 설정")
        keyword_str = st.sidebar.text_area("키워드 (쉼표로 구분)", ", ".join(st.session_state.get('custom_keywords', config.BENCHMARKING_KEYWORDS)))
        st.session_state.custom_keywords = [kw.strip() for kw in keyword_str.split(',')]
        
        if st.button("🔄 최신 벤치마킹 뉴스 수집 및 분석", type="primary"):
            with st.spinner("뉴스 수집 및 AI 분석 중..."):
                collector = SKNewsCollector(custom_keywords=st.session_state.custom_keywords)
                news_df = collector.collect_news()
                st.session_state.news_data = news_df
                if news_df is not None and not news_df.empty:
                    gemini = GeminiInsightGenerator(config.GEMINI_API_KEY)
                    st.session_state.news_insight = gemini.generate_news_insight(news_df)
                else:
                    st.warning("관련 뉴스를 찾지 못했습니다.")
                    st.session_state.news_insight = None
        
        if 'news_insight' in st.session_state and st.session_state.news_insight:
            st.subheader("🤖 AI 종합 분석 리포트")
            st.markdown(st.session_state.news_insight)
        
        if 'news_data' in st.session_state and st.session_state.news_data is not None:
            st.subheader("📋 수집된 뉴스 목록")
            st.dataframe(st.session_state.news_data, use_container_width=True, column_config={"URL": st.column_config.LinkColumn("🔗 Link")})

    with tabs[3]: # 통합 인사이트 탭
        st.subheader("🧠 통합 인사이트 생성")
        
        if st.button("🚀 통합 인사이트 생성", type="primary"):
            if st.session_state.get('financial_insight') and st.session_state.get('news_insight'):
                with st.spinner("재무 인사이트와 뉴스 인사이트를 통합 분석 중..."):
                    gemini = GeminiInsightGenerator(config.GEMINI_API_KEY)
                    st.session_state.integrated_insight = gemini.generate_integrated_insight(
                        st.session_state.financial_insight,
                        st.session_state.news_insight
                    )
                st.success("✅ 통합 인사이트가 생성되었습니다!")
            else:
                st.warning("⚠️ 재무 인사이트와 뉴스 인사이트가 모두 필요합니다. 먼저 재무분석과 뉴스분석을 완료해주세요.")
        
        if 'integrated_insight' in st.session_state and st.session_state.integrated_insight:
            st.subheader("🤖 통합 인사이트 결과")
            st.markdown(st.session_state.integrated_insight)
        else:
            st.info("재무분석과 뉴스분석을 완료한 후 통합 인사이트를 생성할 수 있습니다.")

    with tabs[4]: # 보고서 생성 탭
        st.subheader("📄 통합 보고서 생성 & 이메일 서비스 바로가기")

        # 2열 레이아웃: PDF 생성 + 이메일 입력
        col1, col2 = st.columns([1, 1])

        with col1:
            st.write("**📥 보고서 다운로드**")

            # 👉 사용자 입력(보고 대상/보고자/푸터 노출)
            report_target = st.text_input("보고 대상", value="SK이노베이션 경영진")
            report_author = st.text_input("보고자", value="")
            show_footer = st.checkbox("푸터 문구 표시(※ 본 보고서는 대시보드에서 자동 생성되었습니다.)", value=False)

            # 보고서 형식 선택
            report_format = st.radio("파일 형식 선택", ["PDF", "Excel"], horizontal=True)

            if st.button("📥 보고서 생성", type="primary", key="make_report"):
                # 데이터 우선순위: DART 자동 > 수동 업로드
                financial_data_for_report = None
                if st.session_state.financial_data is not None and not st.session_state.financial_data.empty:
                    financial_data_for_report = st.session_state.financial_data
                elif st.session_state.manual_financial_data is not None and not st.session_state.manual_financial_data.empty:
                    financial_data_for_report = st.session_state.manual_financial_data

                # 선택 입력(있으면 전달)
                quarterly_df = st.session_state.get("quarterly_data")
                selected_charts = st.session_state.get("selected_charts")

                with st.spinner("📄 보고서 생성 중..."):
                    if report_format == "PDF":
                        file_bytes = create_enhanced_pdf_report(
                            financial_data=financial_data_for_report,
                            news_data=st.session_state.news_data,
                            insights=st.session_state.integrated_insight or st.session_state.financial_insight or st.session_state.news_insight,
                            quarterly_df=quarterly_df,                 # 분기 데이터(있으면)
                            selected_charts=selected_charts,           # 외부 전달 차트(있으면)
                            show_footer=show_footer,                   # ✅ 푸터 표시 여부 반영
                            report_target=report_target.strip() or "보고 대상 미기재",  # ✅ 사용자 입력 반영
                            report_author=report_author.strip() or "보고자 미기재"      # ✅ 사용자 입력 반영
                        )
                        filename = "SK_Energy_Analysis_Report.pdf"
                        mime_type = "application/pdf"
                    else:
                        file_bytes = create_excel_report(
                            financial_data=financial_data_for_report,
                            news_data=st.session_state.news_data,
                            insights=st.session_state.integrated_insight or st.session_state.financial_insight or st.session_state.news_insight
                        )
                        filename = "SK_Energy_Analysis_Report.xlsx"
                        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

                    if file_bytes:
                        # 세션에 파일 정보 저장
                        st.session_state.generated_file = file_bytes
                        st.session_state.generated_filename = filename
                        st.session_state.generated_mime = mime_type

                        st.download_button(
                            label="⬇️ 보고서 다운로드",
                            data=file_bytes,
                            file_name=filename,
                            mime=mime_type
                        )
                        st.success("✅ 보고서가 성공적으로 생성되었습니다!")
                    else:
                        st.error("❌ 보고서 생성에 실패했습니다.")
                        
        with col2:
            st.write("**📧 이메일 서비스 바로가기**")

            mail_providers = {
                "네이버": "https://mail.naver.com/",
                "구글(Gmail)": "https://mail.google.com/",
                "다음": "https://mail.daum.net/",
                "네이트": "https://mail.nate.com/",
                "야후": "https://mail.yahoo.com/",
                "아웃룩(Outlook)": "https://outlook.live.com/",
                "프로톤메일(ProtonMail)": "https://mail.proton.me/",
                "조호메일(Zoho Mail)": "https://mail.zoho.com/",
                "GMX 메일": "https://www.gmx.com/",
                "아이클라우드(iCloud Mail)": "https://www.icloud.com/mail",
                "메일닷컴(Mail.com)": "https://www.mail.com/",
                "AOL 메일": "https://mail.aol.com/"
            }

            selected_provider = st.selectbox(
                "메일 서비스 선택",
                list(mail_providers.keys()),
                key="mail_provider_select"
            )
            url = mail_providers[selected_provider]

            st.markdown(
                f"[{selected_provider} 메일 바로가기]({url})",
                unsafe_allow_html=True
            )
            st.info("선택한 메일 서비스 링크가 새 탭에서 열립니다.")

            if st.session_state.get('generated_file'):
                st.download_button(
                    label=f"📥 {st.session_state.generated_filename} 다운로드",
                    data=st.session_state.generated_file,
                    file_name=st.session_state.generated_filename,
                    mime=st.session_state.generated_mime,
                    key="download_generated_report_btn"
                )
            else:
                st.info("먼저 보고서를 생성해주세요.")

if __name__ == "__main__":
    main()
