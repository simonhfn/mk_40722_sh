from src.database import SQLDatabase


def create_schema():
    conn = SQLDatabase()._get_conn()
    conn.cursor().execute(open("schema.sql", "r").read())
    conn.commit()


if __name__ == "__main__":
    create_schema()
    tables = SQLDatabase().get_as_df("SELECT * FROM pg_catalog.pg_tables;")
    print(tables.to_markdown())