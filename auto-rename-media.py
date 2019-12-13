import os
import requests
import database_manager
import gzip
import sys


class RENAME:
    def __init__(self):
        episodes = "Hot to the Touch,Five Short Graybles,Web Weirdos,Dream of Love,Return to the Nightosphere,Daddy's Little Monster,In Your Footsteps,Hug Wolf,Princess Monster Wife,Goliad,Beyond This Earthly Realm,Gotcha!,Princess Cookie,Card Wars,Sons of Mars,Burning Low,BMO Noire,King Worm,Lady & Peebles,You Made Me,Who Would Win,Ignition Point,The Hard Easy,Reign of Gunters,I Remember You"
        episode_array = episodes.split(',')

    def getFiles(self):
        self.files = os.listdir()


class DATASET:
    def __init__(self):
        self.main_url = "https://datasets.imdbws.com/"
        self.names_basics = "name.basics.tsv.gz"
        self.title_akas = "title.akas.tsv.gz"
        self.title_basics = "title.basics.tsv.gz"
        self.title_crew = "title.crew.tsv.gz"
        self.title_episodes = "title.episode.tsv.gz"
        self.title_principles = "title.principals.tsv.gz"
        self.title_ratings = "title.ratings.tsv.gz"
        self.db = database_manager.MANAGER()

    def getNewSet(self):
        self.downloadSet(self.names_basics)
        self.downloadSet(self.title_akas)
        self.downloadSet(self.title_basics)
        self.downloadSet(self.title_crew)
        self.downloadSet(self.title_episodes)
        self.downloadSet(self.title_principles)
        self.downloadSet(self.title_ratings)

    def downloadSet(self, set_type):
        print('Checking ' + set_type)
        response = requests.head(
            self.main_url + set_type).headers['Content-length']
        filename = set_type
        file_size = os.path.getsize(filename)
        if(int(response) != int(file_size)):
            print('Getting ' + set_type)
            response = requests.get(self.main_url + set_type)
            file = open(filename, 'wb')
            file.write(response.content)
            file.close()
        else:
            print('File size matches')

    def writeSet2DB(self):
        self.db.addName('basics')

    def updateType(self, set_type):
        print()

    def decompress(self, filename):
        file = gzip.open(filename, 'rb')
        self.decompressed_file = file.read()
        file.close()
        print(sys.getsizeof(self.decompressed_file))


if __name__ == "__main__":
    # RENAME()
    main = DATASET()
    main.getNewSet()
    main.decompress('imdb_' + main.title_ratings)
