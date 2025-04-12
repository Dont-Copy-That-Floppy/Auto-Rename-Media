# tsv_importer.py

import gzip
import pymongo
import os
from config import DATA_DIR, MONGO_URI, MONGO_DB
from mongo_manager import MongoManager


class TSVImporter:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.manager = MongoManager()

    def import_file(self, filename, batch_size=1000):
        full_path = os.path.join(DATA_DIR, filename)
        record_type = filename.replace(".tsv.gz", "")

        print(f"\nImporting {filename} into MongoDB")

        with gzip.open(full_path, 'rt', encoding='utf-8') as file:
            header = file.readline().strip().split('\t')
            count = 0

            for line in file:
                row = line.strip().split('\t')
                if len(row) != len(header):
                    continue

                data = {header[i]: row[i] for i in range(len(header))}

                if record_type == "name.basics":
                    self.manager.insert_name("basics", data)
                else:
                    self.manager.insert_title(record_type, data)

                count += 1
                if count % 1000 == 0:
                    print(f"Inserted {count} records...", end='\r')

        print(f"✔ Done. Inserted {count} total records from {filename}")
