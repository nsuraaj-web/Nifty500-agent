from dotenv import load_dotenv
load_dotenv()
import bcrypt
from supabaseclient import supabase

def create_user(email: str, full_name: str, password: str):
    try:
        password_hash = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        res = (
            supabase.table("app_users")
            .insert(
                {
                    "email": email,
                    "full_name": full_name,
                    "password_hash": password_hash,
                }
            )
            .execute()
        )

        print("Raw response:", res)
        try:
            print("Data:", res.data)
        except AttributeError:
            pass

    except Exception as e:
        print("Error while creating user:", e)


if __name__ == "__main__":
    create_user(
        "nsuraaj@gmail.com", 
        "Suraaj Nair", 
        "StockRating@1231"
    )
