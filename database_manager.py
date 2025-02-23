import pymongo


class MANAGER:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")

    def setClient(self, client):
        self.imdb = self.client[client]

    def addName(self, type, object):
        if(type == 'basics'):
            """
                nconst (string) - alphanumeric unique identifier of the name/person
                primaryName (string) - name by which the person is most often credited
                birthYear - in YYYY format
                deathYear - in YYYY format if applicable, else '\N'
                primaryProfession (array of strings)- the top-3 professions of the person
                knownForTitles (array of tconsts) - titles the person is known for
            """
            sub_dict = {"nconst": object['nconst'],
                        "primaryName": object['primaryName'],
                        "birthYear": object['birthYear'],
                        "deathYear": object['deathYear'],
                        "primaryProfession": object['primaryProfession'],
                        "knownForTitles": object['knownForTitles']}
            dict = {"basics": sub_dict}

        if(self.imdb["name"].find_one(dict) == None):
            self.imdb["name"].insert(dict)

    def addTitle(self, type, object):
        if('akas' in type):
            """
                titleId (string) - a tconst, an alphanumeric unique identifier of the title
                ordering (integer) - a number to uniquely identify rows for a given titleId
                title (string) - the localized title
                region (string) - the region for this version of the title
                language (string) - the language of the title
                types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
                attributes (array) - Additional terms to describe this alternative title, not enumerated
                isOriginalTitle (boolean) - 0: not original title; 1: original title
            """
            sub_dict = {"titleId": object['titleId'],
                        "ordering": object['ordering'],
                        "title": object['title'],
                        "region": object['region'],
                        "language": object['language'],
                        "types": object['types'],
                        "attributes": object['attributes'],
                        "isOriginalTitle": object['isOriginalTitle']}
            dict = {"akas": sub_dict}
        elif('basics' in type):
            """
                tconst (string) - alphanumeric unique identifier of the title
                titleType (string) - the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
                primaryTitle (string) - the more popular title / the title used by the filmmakers on promotional materials at the point of release
                originalTitle (string) - original title, in the original language
                isAdult (boolean) - 0: non-adult title; 1: adult title
                startYear (YYYY) - represents the release year of a title. In the case of TV Series, it is the series start year
                endYear (YYYY) - TV Series end year. '\N' for all other title types
                runtimeMinutes - primary runtime of the title, in minutes
                genres (string array) - includes up to three genres associated with the title
            """
            sub_dict = {"tconst": object['tconst'],
                        "titleType": object['title'],
                        "primaryTitle": object['primaryTitle'],
                        "originalTitle": object['originalTitle'],
                        "isAdult": object['isAdult'],
                        "startYear": object['startYear'],
                        "endYear": object['endYear'],
                        "runtimeMinutes": object['runtimeMinutes'],
                        "genres": object['genres']}
            dict = {"basics": sub_dict}
        elif('crew' in type):
            """
                tconst (string) - alphanumeric unique identifier of the title
                directors (array of nconsts) - director(s) of the given title
                writers (array of nconsts) - writer(s) of the given title
            """
            sub_dict = {"tconst": object['tconst'],
                        "directors": object['directors'],
                        "writers": object['writers']}
            dict = {"crew": sub_dict}
        elif('episode' in type):
            """
                tconst (string) - alphanumeric identifier of episode
                parentTconst (string) - alphanumeric identifier of the parent TV Series
                seasonNumber (integer) - season number the episode belongs to
                episodeNumber (integer) - episode number of the tconst in the TV series
            """
            sub_dict = {"tconst": object['tconst'],
                        "parentTconst": object['parentTconst'],
                        "seasonNumber": object['seasonNumber'],
                        "episodeNumber": object['episodeNumber']}
            dict = {"episode": sub_dict}
        elif('principals' in type):
            """
                tconst (string) - alphanumeric unique identifier of the title
                ordering (integer) - a number to uniquely identify rows for a given titleId
                nconst (string) - alphanumeric unique identifier of the name/person
                category (string) - the category of job that person was in
                job (string) - the specific job title if applicable, else '\N'
                characters (string) - the name of the character played if applicable, else '\N'
            """
            sub_dict = {"tconst": object['tconst'],
                        "ordering": object['ordering'],
                        "nconst": object['nconst'],
                        "category": object['category'],
                        "job": object['job'],
                        "characters": object['characters']}
            dict = {"principals": sub_dict}
        elif('ratings' in type):
            """
                tconst (string) - alphanumeric unique identifier of the title
                averageRating - weighted average of all the individual user ratings
                numVotes - number of votes the title has received
            """
            sub_dict = {"tconst": object['tconst'],
                        "averageRating": object['averageRating'],
                        "numVotes": object['numVotes']}
            dict = {"ratings": sub_dict}

        # if(self.imdb['title'].countDocuments(dict) == 0):
        if(self.imdb['title'].find(dict).count() == 0):
            self.imdb["title"].insert(dict)

    def deleteCollection(self, collection=None):
        collection.drop()
        result = self.checkCollectionExists(collection)
        if(result is None):
            return ("Collection %s successfully deleted" % collection)
        else:
            return ("Error occured, Collection was not removed")

    def checkDBExists(self, db_name=None):
        dblist = self.client.list_database_names()
        if db_name in dblist:
            return ("The database %s exists." % db_name)

    def checkCollectionExists(self, collection=None):
        collist = self.imdb.list_collection_names()
        if collection in collist:
            return ("The collection %s exists." % collection)
