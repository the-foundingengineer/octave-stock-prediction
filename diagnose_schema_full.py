
from app.database import engine
from sqlalchemy import text, inspect
import json

def diagnose():
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    schema_info = {}
    for table_name in tables:
        columns = inspector.get_columns(table_name)
        schema_info[table_name] = [
            {"name": col["name"], "type": str(col["type"]), "nullable": col["nullable"]}
            for col in columns
        ]
    
    with open("db_schema_actual.json", "w") as f:
        json.dump(schema_info, f, indent=4)
    
    print("Schema info saved to db_schema_actual.json")

    # Also print summary for quick check
    print("\nSummary of tables and columns:")
    for table, cols in schema_info.items():
        print(f"\nTable: {table}")
        for col in cols:
            print(f"  - {col['name']} ({col['type']})")

if __name__ == "__main__":
    diagnose()
