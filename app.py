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


# --- Helper to escape markdown-sensitive characters ---
def escape_md(text: str) -> str:
    """Escape markdown special chars so Streamlit won't misformat $ or _ etc."""
    text = text.replace("\\", "\\\\")  # escape backslashes first
    return re.sub(r'([*$`_])', r'\\\1', text)


# --- Show chat history ---
for msg in st.session_state.chat_history:
    if msg["role"] == "user":
        with st.chat_message("user", avatar="user.png"):
            st.markdown(msg["content"])
    else:
        with st.chat_message("assistant", avatar="businessman.png"):
            st.markdown(msg["content"])


# --- User input ---
question = st.chat_input("Type your question here and hit Enter...")

if question:
    # Grab last user question if exists
    last_user_question = None
    for past in reversed(st.session_state.chat_history):
        if past["role"] == "user":
            last_user_question = past["content"]
            break

    # Display user message
    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user", avatar="user.png"):
        st.markdown(question)

    # Display assistant response container
    with st.chat_message("assistant", avatar="businessman.png"):
        response_container = st.empty()
        response_text = ""

        try:
            # ✅ Pass full session context into gpt_engine
            result = answer_question(
                question=question,
                last_question=last_user_question,
                conversation_summary=st.session_state.conversation_summary,
                scope=st.session_state.scope,
                resolved_question=st.session_state.resolved_question,
                primary_response=st.session_state.primary_response,
            )

            # --- Stream the model's answer (Markdown-safe) ---
            for token in result["stream"]:
                response_text += token
                safe_text = escape_md(response_text)
                response_container.markdown(safe_text + "▌")

            # Final render (remove cursor)
            response_container.markdown(escape_md(response_text))

            # Save chat + SQL
            st.session_state.chat_history.append(
                {"role": "assistant", "content": response_text}
            )
            st.session_state.last_sql = result.get("sql", "")

            # ✅ Persist updated conversation state from engine
            st.session_state.conversation_summary = result.get("conversation_summary")
            st.session_state.scope = result.get("scope", st.session_state.scope)
            st.session_state.resolved_question = result.get("resolved_question")
            st.session_state.primary_response = result.get("primary_response")

            # --- Collapsible Generated SQL ---
            if "sql" in result and result["sql"]:
                with st.expander("📄 Generated SQL", expanded=False):
                    st.code(result["sql"], language="sql")

            # --- Optional table preview ---
            if result.get("rows"):
                df = pd.DataFrame(result["rows"], columns=result.get("columns", []))
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No rows returned.")

        except Exception as e:
            error_msg = f"Error: {e}"
            response_container.markdown(error_msg)
            st.session_state.chat_history.append(
                {"role": "assistant", "content": error_msg}
            )
