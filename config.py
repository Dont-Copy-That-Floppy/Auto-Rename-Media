# config.py

import os

# Local directory with media files
MEDIA_DIR = os.path.abspath("I:/Retitle")  # Adjust this as needed

# MongoDB connection string and database name
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "imdb"

# IMDb dataset base URL
IMDB_DATA_URL = "https://datasets.imdbws.com/"

# IMDb filenames to download
IMDB_FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

# Directory to store downloaded and decompressed IMDb data
DATA_DIR = os.path.abspath("./imdb_data")

# Matching threshold for fuzzy matching (0–100)
FUZZY_MATCH_THRESHOLD = 85
