import streamlit as st
import re
from sshtunnel import SSHTunnelForwarder
from langchain.chains import create_sql_query_chain
from langchain_core.prompts import PromptTemplate
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI  # ✅ Correct import (NEW)

from dotenv import load_dotenv
import os

# ===================== CONFIGURATION ===================== #

load_dotenv()

SSH_HOST = os.getenv("SSH_HOST")
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


# ===================== SQL CHAIN PROMPT ===================== #

def strict_sql_chain(llm, db):
    prompt_text = (
        "You are an expert MySQL query generator.\n\n"
        "Given the user's question and the database schema below, you MUST output a valid SQL query "
        "that can run directly on the connected MySQL database.\n\n"
        "RULES:\n"
        "- Do NOT explain or reason.\n"
        "- Do NOT include any text outside the SQL.\n"
        "- Always wrap your SQL in triple backticks like this:\n"
        "```sql\nSELECT * FROM table_name;\n```\n"
        "- Always use LIKE with wildcards for text searches when the user gives partial or approximate names.\n"
        "- Always use existing column names exactly as shown in the schema.\n"
        "- If unsure about column names, infer logically from the schema.\n\n"
        "Database schema:\n{table_info}\n\n"
        "User question:\n{input}\n\n"
        "Relevant tables:\n{top_k}\n"
    )

    prompt = PromptTemplate.from_template(prompt_text)
    return create_sql_query_chain(llm, db, prompt=prompt)


# ===================== MAIN FUNCTION ===================== #

def dataBase(question: str, llm):
    server = None
    try:
        server = SSHTunnelForwarder(
            (SSH_HOST, 22),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=('127.0.0.1', 3306),
            local_bind_address=('127.0.0.1', 3307),
            allow_agent=False,
            host_pkey_directories=[]
        )
        server.start()
        st.success(f"SSH tunnel established: 127.0.0.1:{server.local_bind_port} → {SSH_HOST}:3306")

        db = SQLDatabase.from_uri(
            f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{server.local_bind_port}/{DB_NAME}"
        )

        schema = db.get_table_info()
        try:
            table_data = db.run("SHOW TABLES;")
            table_names = [t[0] for t in table_data]
            top_k_tables = ", ".join(table_names)
        except Exception:
            top_k_tables = "All database tables"

        chain = strict_sql_chain(llm, db)

        res = chain.invoke({
            "question": question,
            "table_info": schema,
            "top_k": top_k_tables
        })

        res_text = str(res)

        match = re.search(r"```sql\s*(.*?)\s*```", res_text, re.DOTALL | re.IGNORECASE)
        sql_query = match.group(1).strip() if match else res_text.strip()

        sql_query = sql_query.replace("```", "").replace("sql", "").strip()
        if not sql_query.endswith(";"):
            sql_query += ";"

        st.code(sql_query, language="sql")

        try:
            result = db.run(sql_query)
            return result

        except Exception as e:
            error_msg = str(e)
            st.error(f"SQL Execution Error: {error_msg}")

            if "Unknown column" in error_msg or "doesn't exist" in error_msg:
                follow_up = (
                    f"The previous query failed with error: {error_msg}. "
                    f"Rewrite the SQL correctly using only existing columns from this schema:\n{schema}\n"
                    f"Question: {question}"
                )
                retry_res = chain.invoke({
                    "question": follow_up,
                    "table_info": schema,
                    "top_k": top_k_tables
                })
                retry_text = str(retry_res)

                match = re.search(r"```sql\s*(.*?)\s*```", retry_text, re.DOTALL | re.IGNORECASE)
                retry_sql = match.group(1).strip() if match else retry_text.strip()

                if not retry_sql.endswith(";"):
                    retry_sql += ";"

                st.code(retry_sql, language="sql")

                result = db.run(retry_sql)
                return result

            else:
                raise e

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        return None

    finally:
        if server:
            server.stop()
            st.info("SSH tunnel closed.")


# ===================== STREAMLIT UI ===================== #

st.title("🧠 AI SQL Query Generator (via SSH + MySQL)")
st.write("Ask your database anything using natural language.")

question = st.text_input("Enter your question:")

if st.button("Run Query"):
    if question.strip() == "":
        st.warning("Please enter a question.")
    else:
        llm = ChatOpenAI(
            model="gpt-4.1",
            temperature=0,
            api_key=os.getenv("OPENAI_API_KEY")   # ✅ MUST INCLUDE THIS
        )
        result = dataBase(question, llm)

        if result:
            st.success("Query executed successfully!")
            st.write(result)
