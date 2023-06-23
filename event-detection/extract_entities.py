import casanova
from ural import get_domain_name
from ural.lru import LRUTrie


class EntityExtractor(object):

    def __init__(self) -> None:

        categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]

        self.media_domains = LRUTrie()
        media_index = {}
        inverted_media_index = {}

        with casanova.reader('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv') as reader:
            for i, row in enumerate(reader):
                if row[reader.headers.wheel_category] in categories_sorted:
                    media_index[row[reader.headers.name]] = i
                    inverted_media_index[i] = row[reader.headers.name]
                    for prefix in row[reader.headers.prefixes].split('|'):
                        if get_domain_name(prefix) not in [None, 'facebook.com','twitter.com', 'youtube.com', 'dailymotion.com']:
                            self.media_domains.set(prefix, row[reader.headers.name])

        with casanova.reader('https://raw.githubusercontent.com/regardscitoyens/twitter-parlementaires/master/data/deputes.csv') as reader:
            self.mp_ids = dict()
            for i, row in enumerate(reader):
                self.mp_ids[int(row[reader.headers.twitter_id])] = row[reader.headers.nom]


    def extract_media(self, url):
        return self.media_domains.match(url)


    def is_media(self, url):
        return bool(self.extract_media(url))


    def extract_mp(self, user_id):
        return self.mp_ids.get(int(user_id))


    def is_mp(self, user_id):
        return bool(self.extract_mp(user_id))