import unittest

import pandas as pd

from src.analysis import extract_company_from_email, preprocess, perform_analysis_on_events


class testAnalysis(unittest.TestCase):
    def test_extract_company_from_email(self):
        self.assertEqual(extract_company_from_email("aa@google.com"), "google")

    def test_extract_company_from_email_empty(self):
        self.assertEqual(extract_company_from_email(""), "Unknown company")

    def test_preprocess_df(self):
        df = pd.DataFrame({"email": ['aa@google.com', "aa@apple.com"]})
        res_df = pd.DataFrame({"email": ['aa@google.com', "aa@apple.com"],
                               "company": ["google", "apple"]})
        self.assertEqual(preprocess(df).to_dict(), res_df.to_dict())

    def test_perform_analysis_on_events(self):
        df = pd.DataFrame({"email": ['aa@google.com', "aa@apple.com", "aa@apple.com"]})
        res_df = pd.DataFrame({"email": ["aa@apple.com", 'aa@google.com'],
                               "count": [2, 1]})
        self.assertEqual(perform_analysis_on_events(df, {"email"})['email'].to_dict(), res_df.to_dict())

