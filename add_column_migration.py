from app.database import engine
from sqlalchemy import text

def add_column():
    with engine.connect() as connection:
        try:
            print("Adding adjustment_factor column to stocks table...")
            connection.execute(text("ALTER TABLE stocks ADD COLUMN adjustment_factor VARCHAR"))
            connection.commit()
            print("Column added successfully.")
        except Exception as e:
            print(f"Error adding column (it might already exist): {e}")

if __name__ == "__main__":
    add_column()
