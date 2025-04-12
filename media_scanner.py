# media_scanner.py

import os
from config import MEDIA_DIR


class MediaScanner:
    def __init__(self, media_dir=MEDIA_DIR):
        self.media_dir = media_dir

    def scan(self):
        movies = []
        shows = []

        for item in os.listdir(self.media_dir):
            full_path = os.path.join(self.media_dir, item)
            if os.path.isfile(full_path):
                movies.append(item)
            elif os.path.isdir(full_path):
                shows.append(item)

        return {
            "movies": movies,
            "shows": shows
        }
