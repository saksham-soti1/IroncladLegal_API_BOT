# app.py
import streamlit as st
import pandas as pd
import json
import re
from gpt_engine import answer_question

# ===========================
# Page Setup
# ===========================
st.set_page_config(page_title="Ironclad Contract Chatbot", layout="wide")
st.markdown("<h1 style='color:#39FF14;'>Ironclad Contract Chatbot</h1>", unsafe_allow_html=True)
st.markdown("Ask any question about your contracts.")

# ===========================
# Session State Initialization
# ===========================
defaults = {
    "chat_history": [],
    "last_sql": "",
    "conversation_summary": None,
    "scope": {},
    "resolved_question": None,
    "primary_response": None,
    "last_question": None,
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ===========================
# Markdown Escaping
# ===========================
def escape_md(text: str) -> str:
    """Escape markdown special chars so Streamlit won't misformat $ or _ etc."""
    text = text.replace("\\", "\\\\")
    return re.sub(r'([*$`_])', r'\\\1', text)

# ===========================
# Display Chat History
# ===========================
for msg in st.session_state.chat_history:
    avatar = "user.png" if msg["role"] == "user" else "businessman.png"
    with st.chat_message(msg["role"], avatar=avatar):
        st.markdown(msg["content"])

# ===========================
# User Input
# ===========================
question = st.chat_input("Type your question here and hit Enter...")

if question:
    # -----------------------
    # Save user message
    # -----------------------
    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user", avatar="user.png"):
        st.markdown(question)

    # -----------------------
    # Prepare container for model response
    # -----------------------
    with st.chat_message("assistant", avatar="businessman.png"):
        response_container = st.empty()
        response_text = ""

        try:
            # ✅ Pass full persistent state into gpt_engine
            result = answer_question(
                question=question,
                last_question=st.session_state.last_question,
                conversation_summary=st.session_state.conversation_summary,
                scope=st.session_state.scope,
                resolved_question=st.session_state.resolved_question,
                primary_response=st.session_state.primary_response,
            )

            # -----------------------
            # Stream response tokens
            # -----------------------
            for token in result["stream"]:
                response_text += token
                safe_text = escape_md(response_text)
                response_container.markdown(safe_text + "▌")
            response_container.markdown(escape_md(response_text))

            # -----------------------
            # Save assistant response
            # -----------------------
            st.session_state.chat_history.append(
                {"role": "assistant", "content": response_text}
            )
            st.session_state.last_sql = result.get("sql", "")

            # ✅ Persist conversation context
            st.session_state.conversation_summary = result.get("conversation_summary")
            st.session_state.scope = result.get("scope", st.session_state.scope)
            st.session_state.resolved_question = result.get("resolved_question")
            st.session_state.primary_response = result.get("primary_response")
            st.session_state.last_question = question  # <-- crucial persistence line

            # -----------------------
            # Show Generated SQL
            # -----------------------
            if result.get("sql"):
                with st.expander("📄 Generated SQL", expanded=False):
                    st.code(result["sql"], language="sql")

            # -----------------------
            # Show Data Preview
            # -----------------------
            if result.get("rows"):
                df = pd.DataFrame(result["rows"], columns=result.get("columns", []))
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No rows returned.")

        except Exception as e:
            error_msg = f"⚠️ Error: {e}"
            response_container.markdown(error_msg)
            st.session_state.chat_history.append(
                {"role": "assistant", "content": error_msg}
            )
