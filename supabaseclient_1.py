# supabase_client.py
from supabase import create_client
from myapp.StockRating.config.config import SUPABASE_URL, SUPABASE_KEY

# Initialise Supabase client using values from config.py / .env
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_consolidated(limit: int = 1000):
    """
    Core fundamentals, prices, margins etc from consolidated_master.
    One row per ticker (latest snapshot).
    """
    resp = supabase.table("consolidated_master").select("*").limit(limit).execute()
    return resp.data

def fetch_derived(limit: int = 1000):
    """
    Derived metrics like returns, growth %, PEG etc.
    """
    resp = supabase.table("derived_master").select("*").limit(limit).execute()
    return resp.data

def fetch_ratings(limit: int = 1000):
    """
    Your internal rating system output.
    """
    resp = supabase.table("ratings_master").select("*").limit(limit).execute()
    return resp.data

def fetch_screenr(limit: int = 1000):
    """
    Ownership, efficiency, ROCE, OPM etc from screenr_master.
    """
    resp = supabase.table("screenr_master").select("*").limit(limit).execute()
    return resp.data

# optional helpers if you want them later
def fetch_master_universe(limit: int = 1000):
    resp = supabase.table("master_universe").select("*").limit(limit).execute()
    return resp.data

def fetch_yfin(limit: int = 1000):
    resp = supabase.table("yfin_master").select("*").limit(limit).execute()
    return resp.data
