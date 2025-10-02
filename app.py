# app.py
import streamlit as st
import pandas as pd
import json
from gpt_engine import answer_question

st.set_page_config(page_title="Ironclad Contract Chatbot", layout="wide")
st.markdown("<h1 style='color:#39FF14;'>Ironclad Contract Chatbot</h1>", unsafe_allow_html=True)
st.markdown("Ask any question about your contracts.")

# --- Session State ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "last_sql" not in st.session_state:
    st.session_state.last_sql = ""

# --- Show chat history ---
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
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

    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        response_container = st.empty()
        response_text = ""

        try:
            # ✅ Pass last_user_question into gpt_engine
            result = answer_question(question, last_question=last_user_question)

            # --- Stream answer directly from result["stream"] ---
            for token in result["stream"]:
                response_text += token
                response_container.markdown(response_text + "▌")
            response_container.markdown(response_text)

            st.session_state.chat_history.append(
                {"role": "assistant", "content": response_text}
            )
            st.session_state.last_sql = result["sql"]

            # --- Show SQL + table preview ---
            st.markdown("### Generated SQL")
            st.code(result["sql"], language="sql")

            if result["rows"]:
                df = pd.DataFrame(result["rows"], columns=result["columns"])
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No rows returned.")

            # --- Debug: show classifier output ---
            with st.expander("🔍 Intent JSON (debug)"):
                st.code(json.dumps(result.get("intent_json", {}), indent=2), language="json")

        except Exception as e:
            error_msg = f"Error: {e}"
            response_container.markdown(error_msg)
            st.session_state.chat_history.append(
                {"role": "assistant", "content": error_msg}
            )
