# llm_setup.py
import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

def get_llm():
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",      # <- current recommended chat model
        google_api_key=GOOGLE_API_KEY,
        temperature=0.2,
    )

def get_embeddings():
    return GoogleGenerativeAIEmbeddings(
        model="models/text-embedding-004",
        google_api_key=GOOGLE_API_KEY,
    )
