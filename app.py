# app.py
import streamlit as st
import pandas as pd
import json
import re
from gpt_engine import answer_question

st.set_page_config(page_title="Ironclad Contract Chatbot", layout="wide")
st.markdown("<h1 style='color:#39FF14;'>Ironclad Contract Chatbot</h1>", unsafe_allow_html=True)
st.markdown("Ask any question about your contracts.")

# --- Session State ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "last_sql" not in st.session_state:
    st.session_state.last_sql = ""

# ✅ persistent conversation state
if "conversation_summary" not in st.session_state:
    st.session_state.conversation_summary = None
if "scope" not in st.session_state:
    st.session_state.scope = {}
if "resolved_question" not in st.session_state:
    st.session_state.resolved_question = None
if "primary_response" not in st.session_state:
    st.session_state.primary_response = None

# --- Adaptive dark/light CSS ---
st.markdown("""
<style>
.assistant-box {
    padding: 12px;
    border-radius: 8px;
    font-size: 1.05rem;
    line-height: 1.6;
    white-space: pre-wrap;
}

/* adapt to theme */
[data-theme="dark"] .assistant-box {
    background-color: #1e1e1e;
    color: #f1f1f1;
    border: 1px solid #333;
}

[data-theme="light"] .assistant-box {
    background-color: #f9f9f9;
    color: #000000;
    border: 1px solid #ddd;
}
</style>
""", unsafe_allow_html=True)

# --- Helper to sanitize markdown safely ---
def sanitize_stream_text(text: str) -> str:
    # Escape markdown symbols that break formatting but keep $
    text = re.sub(r'([*_`])', r'\\\1', text)
    return text.replace("\n", "  \n")

# --- Show chat history ---
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            st.markdown(f"<div class='assistant-box'>{sanitize_stream_text(msg['content'])}</div>", unsafe_allow_html=True)
        else:
            st.markdown(msg["content"])

# --- User input ---
question = st.chat_input("Type your question here and hit Enter...")

if question:
    # Grab last user question if exists
    last_user_question = next((m["content"] for m in reversed(st.session_state.chat_history) if m["role"] == "user"), None)

    # Display user message
    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Assistant streaming
    with st.chat_message("assistant"):
        response_container = st.empty()
        response_text = ""

        try:
            result = answer_question(
                question=question,
                last_question=last_user_question,
                conversation_summary=st.session_state.conversation_summary,
                scope=st.session_state.scope,
                resolved_question=st.session_state.resolved_question,
                primary_response=st.session_state.primary_response,
            )

            for token in result["stream"]:
                response_text += token
                safe_text = sanitize_stream_text(response_text)
                response_container.markdown(f"<div class='assistant-box'>{safe_text}▌</div>", unsafe_allow_html=True)
            response_container.markdown(f"<div class='assistant-box'>{sanitize_stream_text(response_text)}</div>", unsafe_allow_html=True)

            # Save chat + SQL
            st.session_state.chat_history.append({"role": "assistant", "content": response_text})
            st.session_state.last_sql = result.get("sql", "")

            # Persist state
            st.session_state.conversation_summary = result.get("conversation_summary")
            st.session_state.scope = result.get("scope", st.session_state.scope)
            st.session_state.resolved_question = result.get("resolved_question")
            st.session_state.primary_response = result.get("primary_response")

            # SQL viewer
            if "sql" in result and result["sql"]:
                with st.expander("📄 Generated SQL", expanded=False):
                    st.code(result["sql"], language="sql")

            # Table display
            if result.get("rows"):
                df = pd.DataFrame(result["rows"], columns=result.get("columns", []))
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No rows returned.")

        except Exception as e:
            error_msg = f"Error: {e}"
            response_container.markdown(error_msg)
            st.session_state.chat_history.append({"role": "assistant", "content": error_msg})
