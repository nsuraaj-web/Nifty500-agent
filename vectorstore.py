from langchain_community.vectorstores import Chroma
from llm_setup import get_embeddings

PERSIST_DIR = "./chroma_store"
COLLECTION_NAME = "stocks"

def get_vectorstore():
    embeddings = get_embeddings()
    vs = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=PERSIST_DIR,
    )
    return vs
