# media_matcher.py

import pymongo
from config import MONGO_URI, MONGO_DB, FUZZY_MATCH_THRESHOLD
from utils import clean_filename, fuzzy_match


class MediaMatcher:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.title_collection = self.db["title-basics"]

        # Only pull relevant types (movie or tvSeries for now)
        self.title_docs = list(
            self.title_collection.find({"titleType": {"$in": ["movie", "tvSeries"]}}, {"primaryTitle": 1, "originalTitle": 1, "_id": 1})
        )
        self.title_names = [doc["primaryTitle"] for doc in self.title_docs]

    def match_file(self, filename):
        cleaned = clean_filename(filename)
        match_title, score = fuzzy_match(cleaned, self.title_names, threshold=FUZZY_MATCH_THRESHOLD)
        if match_title:
            matched_doc = next(doc for doc in self.title_docs if doc["primaryTitle"] == match_title)
            return {
                "file": filename,
                "matched_title": matched_doc["primaryTitle"],
                "imdb_id": matched_doc["_id"],
                "score": score
            }
        return {
            "file": filename,
            "matched_title": None,
            "imdb_id": None,
            "score": score
        }

    def match_batch(self, filenames):
        return [self.match_file(name) for name in filenames]
