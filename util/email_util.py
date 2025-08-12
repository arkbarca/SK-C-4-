# -*- coding: utf-8 -*-
import streamlit as st
import config

def create_email_ui():
    """이메일 서비스 바로가기 UI를 생성"""
    st.write("**📧 이메일 서비스 바로가기**")

    selected_provider = st.selectbox(
        "메일 서비스 선택",
        list(config.MAIL_PROVIDERS.keys()),
        key="mail_provider_select"
    )
    url = config.MAIL_PROVIDERS[selected_provider]

    st.markdown(
        f'<a href="{url}" target="_blank" style="background-color: #E31E24; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block; border-radius: 5px;">{selected_provider} 메일 열기</a>',
        unsafe_allow_html=True
    )
    st.info("선택한 메일 서비스가 새 탭에서 열립니다. 생성된 보고서를 다운로드하여 직접 첨부해주세요.")

# 실제 이메일 발송 기능은 보안 문제(ID/PW 노출)로 포함하지 않는 것이 좋습니다.
# 꼭 필요하다면 SMTP 라이브러리를 사용하되, ID/PW는 st.secrets를 통해 안전하게 관리해야 합니다.