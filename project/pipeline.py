{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "d5d87199-a141-4a44-8011-282722906659",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Processing chicago_building_permits dataset...\n",
      "Processing cdc_data dataset...\n",
      "Pipeline completed. Data saved to /data\\chicago_cdc.db\n"
     ]
    }
   ],
   "source": [
    "# File: /project/pipeline.py\n",
    "import io\n",
    "import os\n",
    "import requests\n",
    "import pandas as pd\n",
    "import sqlite3\n",
    "\n",
    "# Configuration\n",
    "DATA_URLS = {\n",
    "    \"chicago_building_permits\": \"https://data.cityofchicago.org/api/views/6irb-gasv/rows.csv?accessType=DOWNLOAD\",\n",
    "    \"cdc_data\": \"https://data.cdc.gov/api/views/hk9y-quqm/rows.csv?accessType=DOWNLOAD\"\n",
    "}\n",
    "DATA_DIR = \"/data\"\n",
    "DB_FILE = os.path.join(DATA_DIR, \"chicago_cdc.db\")\n",
    "\n",
    "def download_data(url):\n",
    "    \"\"\"Download dataset from a URL and return it as a pandas DataFrame.\"\"\"\n",
    "    response = requests.get(url)\n",
    "    response.raise_for_status()  # Raises error for bad requests\n",
    "    data = pd.read_csv(io.StringIO(response.text))\n",
    "    return data\n",
    "\n",
    "def clean_chicago_data(df):\n",
    "    \"\"\"Clean and transform Chicago building permits data.\"\"\"\n",
    "    # Drop rows with missing permit numbers or types\n",
    "    df.fillna(0)\n",
    "    df['Week End'] = pd.to_datetime(df['Week End'], errors='coerce')\n",
    "    df.drop_duplicates(inplace=True)\n",
    "    return df\n",
    "\n",
    "def clean_cdc_data(df):\n",
    "    \"\"\"Clean and transform CDC data.\"\"\"\n",
    "    # Assume 'Date' and 'State' are important columns in CDC data\n",
    "    df.fillna(0)\n",
    "    df['Data As Of'] = pd.to_datetime(df['Data As Of'], errors='coerce')\n",
    "    df.drop_duplicates(inplace=True)\n",
    "    return df\n",
    "\n",
    "def save_to_sqlite(df, table_name, db_file):\n",
    "    \"\"\"Save the DataFrame to an SQLite database.\"\"\"\n",
    "    with sqlite3.connect(db_file) as conn:\n",
    "        df.to_sql(table_name, conn, if_exists=\"replace\", index=False)\n",
    "\n",
    "def main():\n",
    "    \"\"\"Main pipeline function.\"\"\"\n",
    "    # Ensure data directory exists\n",
    "    os.makedirs(DATA_DIR, exist_ok=True)\n",
    "    \n",
    "    # Download, clean, and save each dataset\n",
    "    for name, url in DATA_URLS.items():\n",
    "        print(f\"Processing {name} dataset...\")\n",
    "        \n",
    "        # Step 1: Download data\n",
    "        df = download_data(url)\n",
    "        \n",
    "        # Step 2: Clean data\n",
    "        if name == \"chicago_building_permits\":\n",
    "            df = clean_chicago_data(df)\n",
    "        elif name == \"cdc_data\":\n",
    "            df = clean_cdc_data(df)\n",
    "        \n",
    "        # Step 3: Save data to SQLite\n",
    "        save_to_sqlite(df, name, DB_FILE)\n",
    "    \n",
    "    print(f\"Pipeline completed. Data saved to {DB_FILE}\")\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    main()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "317f983c-6611-402f-94fa-cc0440bf34e9",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
