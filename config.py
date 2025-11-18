import os
from dotenv import load_dotenv

# --- 1. Calculate Project Root Path ---
config_dir = os.path.dirname(os.path.abspath(__file__))
project_root_unresolved = os.path.join(config_dir, '..', '..', '..')

# Force Python to resolve the path for printing/debugging
project_root_resolved = os.path.abspath(project_root_unresolved)

# --- 2. Load .env File ---
load_dotenv(os.path.join(project_root_resolved, '.env')) 

# --- 3. Retrieve Variables ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
