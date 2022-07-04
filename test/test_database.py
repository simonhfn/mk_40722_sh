import unittest

from src.database import SQLDatabase

class DbTest(unittest.TestCase):
    def setUp(cls) -> None:
        print("creating table")
        db = SQLDatabase()
        query = "CREATE TABLE test_table(_id int);"
        db._get_cur().execute(query)
        db.commit()

    def tearDown(self) -> None:
        print("dropping table")
        db = SQLDatabase()
        query = "DROP TABLE test_table;"
        db._get_cur().execute(query)
        db.commit()

    def test_db_get_as_df(self):
        db = SQLDatabase()
        query = "SELECT * FROM pg_catalog.pg_tables;"
        res = db.get_as_df(query)
        self.assertTrue(len(res) > 0)

    def test_insert_values(self):
        db = SQLDatabase()
        query = """INSERT INTO test_table (
                    _id
                    ) VALUES %s;"""
        db.insert(query, [(1,)])
        db.commit()

        res = db.get_as_df("""select * from test_table""")
        self.assertEqual(len(res), 1)

