import unittest

import pandas as pd

from src.database import SQLDatabase

class DbTest(unittest.TestCase):
    def test_connection(self):
        db = SQLDatabase()
        query = "SELECT * FROM pg_catalog.pg_tables;"
        res = db.get_as_df(query)
        self.assertTrue(len(res) > 0)

