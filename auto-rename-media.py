import os
import requests
import gzip
import sys
import pymongo


class RENAME:
    def __init__(self):
        retitle_dir = 'I:/Retitle/'
        tv_shows = []
        movies = []
        for file in os.listdir(retitle_dir):
            if(os.path.isfile(file)):
                movies.append(file)
            else:
                tv_shows.append(file)

        print(tv_shows)
        season_url = 'https://www.imdb.com/title/tt0103359/episodes?season=1'

    def getFiles(self):
        self.files = os.listdir()


class DATASET:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.imdb = self.client["imdb"]

        self.main_url = "https://datasets.imdbws.com/"
        filenames = ["name.basics.tsv.gz", "title.akas.tsv.gz", "title.basics.tsv.gz",
                     "title.crew.tsv.gz", "title.episode.tsv.gz", "title.principals.tsv.gz", "title.ratings.tsv.gz"]
        #self.getNewSet(filenames)
        self.updateAllTables(filenames)

    def updateAllTables(self, filenames):
        for filename in filenames:
            self.setData(filename)

    def addTable(self, filenames, index):
        self.setData(filenames[index])

    def setData(self, filename):
        print('Setting Collection %s documents' % filename[:-len('.tsv.gz')])
        collection_name = "%s" % filename[:-len('.tsv.gz')].replace('.', '-')
        multiple_elements = False
        file_size = os.path.getsize(filename[:-len('.gz')])
        with open(filename[:-len('.gz')], encoding='utf-8') as file:
            file_progression = int(file.tell() / file_size * 100)
            print("Inserting records into %s     %s%% complete\r" %
                  (collection_name, file_progression), end="", flush=True)

            line = file.readline().replace('\n', '')
            header_elements = line.split('\t')
            multiple_elements = (header_elements[1] == 'ordering')

            while line:
                line = file.readline().replace('\n', '')
                elements = line.split('\t')
                if(not multiple_elements):
                    document = {'_id': '%s' % elements[0]}

                    for index in range(1, len(header_elements)):
                        try:
                            document.update(
                                {'%s' % header_elements[index]: '%s' % elements[index]})
                        except:
                            pass
                else:
                    document_id = elements[0]

                    document = {'_id': '%s' % elements[0]}
                    while True:
                        ordering_dict = {'%s' % elements[1]: {}}
                        for index in range(2, len(header_elements)):
                            try:
                                ordering_dict['%s' % elements[1]].update(
                                    {'%s' % header_elements[index]: '%s' % elements[index]})
                            except:
                                pass

                        temp_position = file.tell()
                        line = file.readline().replace('\n', '')
                        elements = line.split('\t')
                        current_document_id = elements[0]
                        if(current_document_id == document_id):
                            document.update(ordering_dict)
                        else:
                            document.update(ordering_dict)
                            file.seek(temp_position)
                            break

                try:
                    self.imdb[collection_name].insert_one(document)
                except Exception as e:
                    # print(e)
                    pass

                if(int(file.tell() / file_size * 100) >= file_progression + 1):
                    file_progression = int(file.tell() / file_size * 100)
                    print("Inserting records into %s     %s%% complete\r" %
                          (collection_name, file_progression), end="", flush=True)

    def getNewSet(self, filenames):
        for filename in filenames:
            self.downloadSet(filename)

    def downloadSet(self, filename):
        if(not os.path.exists('./' + filename)):
            print("%s does not exist.\nDownloading %s..." %
                  (filename, filename))
            self.download(filename)
            self.decompress(filename)
            self.setData(filename)
        elif(self.downloadFileSize(filename) != os.path.getsize(filename)):
            print('%s exists, and content size mismatch.\nDownloading %s...' % (filename, filename))
            if(os.path.exists(filename[:-len('.gz')] + '.old')):
                os.remove(filename[:-len('.gz')] + '.old')
                os.rename(filename[:-len('.gz')], filename[:-len('.gz')] + '.old')
            self.download(filename)
            self.decompress(filename)
            self.compare(filename)
            self.setData(filename)
        else:
            print('%s exists, and size matches' % filename)

    def compare(self, filename):
        if(os.path.exists(filename[:-len('.gz')] + '.new')):
            os.remove(filename[:-len('.gz')] + '.new')
        
        os.rename(filename[:-len('.gz')], filename[:-len('.gz')] + '.new')
        old_file = open(filename[:-len('.gz')] + '.old', encoding='utf-8')
        file = open(filename[:-len('.gz')], 'w')

        with open(filename[:-len('.gz')] + '.new', encoding='utf-8') as new_file:
            old_file_cache = old_file.readline()
            new_file_cache = new_file.readline()
            while new_file_cache:
                if(new_file_cache != old_file_cache):
                    file.write(new_file_cache)
                    new_file_cache = new_file.readline()
                else:
                    old_file_cache = old_file.readline()
                    new_file_cache = new_file.readline()

        old_file.close()
        file.close()


    def downloadFileSize(self, filename):
        response = requests.get(self.main_url + filename,
                                stream=True).headers['Content-length']
        return int(response)

    def download(self, filename):
        response = requests.get(self.main_url + filename)
        with open(filename, 'wb') as file:
            file.write(response.content)

    def decompress(self, filename):
        with open(filename[:-len('.gz')], 'wb') as uFile:
            with gzip.open(filename, 'rb') as file:
                file_cache = file.read(1024)
                uFile.write(file_cache)
                while file_cache:
                    file_cache = file.read(1024)
                    uFile.write(file_cache)

        print('Decompressed: %s' % filename)


if __name__ == "__main__":
    # RENAME()
    DATASET()
