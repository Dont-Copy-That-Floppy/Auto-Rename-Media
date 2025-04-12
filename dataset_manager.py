# dataset_manager.py

import os
import requests
import gzip
import shutil
import pymongo
from config import IMDB_DATA_URL, IMDB_FILES, DATA_DIR, MONGO_URI, MONGO_DB
from utils import tsv_to_documents


class DatasetManager:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        os.makedirs(DATA_DIR, exist_ok=True)

    def update_all(self):
        for filename in IMDB_FILES:
            self.download_if_needed(filename)
            self.decompress(filename)
            self.insert_into_db(filename)

    def download_if_needed(self, filename):
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            print(f"Downloading {filename}...")
            response = requests.get(IMDB_DATA_URL + filename)
            with open(path, 'wb') as f:
                f.write(response.content)

    def decompress(self, filename):
        gz_path = os.path.join(DATA_DIR, filename)
        out_path = os.path.join(DATA_DIR, filename.replace(".gz", ""))
        if not os.path.exists(out_path):
            with gzip.open(gz_path, 'rb') as f_in:
                with open(out_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Decompressed {filename}")

    def insert_into_db(self, filename):
        tsv_path = os.path.join(DATA_DIR, filename.replace(".gz", ""))
        collection_name = filename.replace(".tsv.gz", "").replace('.', '-')
        collection = self.db[collection_name]

        print(f"Inserting data into MongoDB collection: {collection_name}")
        documents = tsv_to_documents(tsv_path)
        if documents:
            collection.drop()  # Drop and recreate
            collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into {collection_name}")
