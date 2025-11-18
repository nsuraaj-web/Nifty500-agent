# ingest_supabase.py
from merge_data import build_merged_stock_data
from build_docs import merged_rows_to_documents
from vectorstore import get_vectorstore


def ingest_all(limit: int = 1000):
    print(f"Fetching and merging data for up to {limit} stocks...")
    merged_rows = build_merged_stock_data(limit=limit)

    print(f"Building Documents for {len(merged_rows)} stocks...")
    docs = merged_rows_to_documents(merged_rows)

    print("Connecting to Chroma vector store...")
    vs = get_vectorstore()

    print(f"Adding {len(docs)} documents to Chroma collection 'stocks'...")
    vs.add_documents(docs)

    print("Done. Chroma has been updated with the latest stock snapshots.")


if __name__ == "__main__":
    ingest_all(limit=1000)
