import pandas as pd
from ural import get_domain_name
from ural.lru import LRUTrie


class EntityExtractor(object):

    def __init__(self) -> None:

        medias = pd.read_csv('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv')
        categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]

        self.media_domains = LRUTrie()
        media_index = {}
        inverted_media_index = {}
        for i, row in medias.iterrows():
            if row['wheel_category'] in categories_sorted:
                media_index[row["name"]] = i
                inverted_media_index[i] = row["name"]
                for prefix in row['prefixes'].split('|'):
                    if get_domain_name(prefix) not in [None, 'facebook.com','twitter.com', 'youtube.com', 'dailymotion.com']:
                        self.media_domains.set(prefix, row['name'])

        mp = pd.read_csv('https://raw.githubusercontent.com/regardscitoyens/twitter-parlementaires/master/data/deputes.csv')
        self.mp_ids = dict()
        for i, row in mp.iterrows():
            self.mp_ids[int(row["twitter_id"])] = row["nom"]



    def extract_media(self, url):
        return self.media_domains.match(url)
    
    def is_media(self, url):
        return bool(self.extract_media(url))


    def extract_mp(self, user_id):
        return self.mp_ids.get(int(user_id))
    
    def is_mp(self, user_id):
        return bool(self.extract_mp(user_id))