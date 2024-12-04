import unittest
import pandas as pd
import os
import sqlite3
from unittest.mock import patch, MagicMock
from pipeline import (
    download_data,
    clean_chicago_data,
    clean_cdc_data,
    save_to_sqlite,
    save_to_csv,
    DATA_URLS,
    DB_FILE
)


class TestPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up any pre-test configurations."""
        cls.sample_chicago_data = pd.DataFrame({
            "Permit Number": [12345, None, 67890],
            "Permit Type": ["New", "Renovation", None],
            "Week End": ["2023-01-01", "2023-01-08", None]
        })

        cls.sample_cdc_data = pd.DataFrame({
            "State": ["CA", "NY", None],
            "Data As Of": ["2023-01-01", None, "Invalid Date"],
            "Cases": [100, 200, None]
        })

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        # Remove SQLite DB file if exists
        try:
            import os
            if os.path.exists(DB_FILE):
                os.remove(DB_FILE)
        except Exception as e:
            print(f"Error during cleanup: {e}")

    @patch("requests.get")
    def test_download_data(self, mock_get):
        """Test downloading data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "col1,col2\nval1,val2\nval3,val4"
        mock_get.return_value = mock_response

        df = download_data(DATA_URLS["chicago_building_permits"])

        # Verify the DataFrame structure and content
        self.assertEqual(list(df.columns), ["col1", "col2"])
        self.assertEqual(len(df), 2)

    def test_clean_chicago_data(self):
        """Test cleaning Chicago building permits data."""
        cleaned_df = clean_chicago_data(self.sample_chicago_data)

        # Check that missing rows are dropped
        self.assertEqual(len(cleaned_df), 2)
        self.assertTrue("Week End" in cleaned_df.columns)

        # Check that Weekend is converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df["Week End"]))

    def test_clean_cdc_data(self):
        """Test cleaning CDC data."""
        cleaned_df = clean_cdc_data(self.sample_cdc_data)

        # Check that missing rows are dropped
        self.assertEqual(len(cleaned_df), 2)
        self.assertTrue("Data As Of" in cleaned_df.columns)

        # Check that Data As Of is converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df["Data As Of"]))

    def test_save_to_sqlite(self):
        """Test saving a DataFrame to SQLite."""
        sample_data = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        table_name = "test_table"

        save_to_sqlite(sample_data, table_name, DB_FILE)

        # Verify the data was saved in SQLite
        with sqlite3.connect(DB_FILE) as conn:
            result = conn.execute(f"SELECT * FROM {table_name}")
            rows = result.fetchall()
            self.assertEqual(len(rows), 3)  # Three rows were inserted

    def test_save_to_csv(self):
        """Test saving a DataFrame to CSV."""
        sample_data = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        file_name = "test_output.csv"

        save_to_csv(sample_data, file_name)

        # Verify the file was created and matches the DataFrame
        saved_df = pd.read_csv(file_name)
        pd.testing.assert_frame_equal(sample_data, saved_df)

    @patch("pipeline.download_data")
    def test_pipeline_main(self, mock_download_data):
        """Test the entire pipeline."""
        # Mock the data downloads
        mock_download_data.side_effect = [
            self.sample_chicago_data,
            self.sample_cdc_data
        ]

        # Run the pipeline
        from pipeline import main
        main()

        # Verify SQLite and CSV files were created
        with sqlite3.connect(DB_FILE) as conn:
            result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in result.fetchall()]
            self.assertIn("chicago_building_permits", tables)
            self.assertIn("cdc_data", tables)

        for name in DATA_URLS:
            csv_file = f"{name}.csv"
            self.assertTrue(os.path.exists(csv_file))


if __name__ == "__main__":
    unittest.main()
