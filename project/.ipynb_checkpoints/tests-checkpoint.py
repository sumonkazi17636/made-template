import unittest
import os
import pandas as pd
import sqlite3
import sys

sys.path.append('./pipeline.py')  # Adjust this path to where your pipeline script is located
import pipeline

class TestPipelineFunctions(unittest.TestCase):

    def test_download_data(self):
        """Test downloading data."""
        for name, url in pipeline.DATA_URLS.items():
            df = pipeline.download_data(url)
            self.assertIsInstance(df, pd.DataFrame, f"Failed: {name} data did not download correctly.")
            print(f"Success: Downloaded {name} data correctly.")

    def test_clean_data(self):
        """Test cleaning data."""
        for name, url in pipeline.DATA_URLS.items():
            df = pipeline.download_data(url)
            if name == "chicago_building_permits":
                df = pipeline.clean_chicago_data(df)
            elif name == "cdc_data":
                df = pipeline.clean_cdc_data(df)
            self.assertFalse(df.isnull().values.any(), f"Failed: {name} data contains null values after cleaning.")
            self.assertFalse(df.duplicated().any(), f"Failed: {name} data contains duplicates after cleaning.")
            print(f"Success: Cleaned {name} data correctly.")

    def test_save_to_sqlite(self):
        """Test saving data to SQLite."""
        df = pd.DataFrame({'test_column': [1, 2, 3]})
        pipeline.save_to_sqlite(df, 'test_table', pipeline.DB_FILE)
        conn = sqlite3.connect(pipeline.DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        self.assertIn('test_table', tables, "Failed: test_table does not exist in the database.")
        print("Success: Data saved to SQLite correctly.")
        conn.close()

    def test_save_to_csv(self):
        """Test saving data to CSV."""
        df = pd.DataFrame({'test_column': [1, 2, 3]})
        csv_file = os.path.join(pipeline.DATA_DIR, 'test_output.csv')
        pipeline.save_to_csv(df, csv_file)
        self.assertTrue(os.path.exists(csv_file), "Failed: CSV file does not exist.")
        print("Success: Data saved to CSV correctly.")
        os.remove(csv_file)  # Clean up

    def test_full_pipeline_execution(self):
        """System-level test that runs the full data pipeline and checks output files."""
        pipeline.main()  # Run the full pipeline

        # Check for the existence of SQLite database file
        self.assertTrue(os.path.exists(pipeline.DB_FILE), "Failed: SQLite database file does not exist.")

        # Check for the existence of each CSV file
        for name in pipeline.DATA_URLS.keys():
            csv_file_path = os.path.join(pipeline.DATA_DIR, f"{name}.csv")
            self.assertTrue(os.path.exists(csv_file_path), f"Failed: CSV file {csv_file_path} does not exist.")
            print(f"Success: {name} CSV file created correctly.")

        # Clean up: Remove CSV files after test
        for name in pipeline.DATA_URLS.keys():
            os.remove(os.path.join(pipeline.DATA_DIR, f"{name}.csv"))
        print("All output files checked and cleaned up successfully.")

if __name__ == '__main__':
    unittest.main()
