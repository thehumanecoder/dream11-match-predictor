import json
import os
import requests
import configparser

from pprint import pprint

from pymongo import MongoClient


class DQModule:
    def __init__(self) -> None:
        url_file_path = os.path.join('configs', 'urls.json')
        with open(url_file_path, 'r', encoding='utf-8') as file:
            self.urls = json.load(file)

        self.config = configparser.ConfigParser()
        self.config.read(os.path.join('configs', 'config.ini'))
        mongo_url = self.config['MongoDB']['url']
        client = self.config['MongoDB']['client']
        self.client = MongoClient(mongo_url)
        self.db = self.client[client]
        self.match_ids = []
        self.match_urls = {}

    def get_data(self) -> None:
        try:
            match_details_url = self.urls['baseUrl'] + \
                self.urls['onMatchLinks']
            pprint("Updating schedules")
            match_details = self.get_match_details(match_details_url)

            if match_details:
                pprint("schedules acquired")
                pprint("Fetching schedules")
                self.get_match_ids()

                pprint(self.match_ids)
                pprint("match ids acquired \n configuring new urls")

                self.configure_match_urls()

        except Exception as e:
            pprint(e)

    def get_match_details(self, match_details_url) -> any:
        try:
            results = requests.get(match_details_url, timeout=15000)
            cleaned_data_str = results.content.decode(
                "utf-8").replace("onMatchLinks(", "").rstrip(");'")
            match_schedules = json.loads(cleaned_data_str)

            # inserting schedules to mongodb
            collection = self.db[self.config['MongoDB']['schedules']]
            collection.drop()
            collection.insert_many(match_schedules)
            return True

        except Exception as e:
            print("Exception occured while getting match schedules:", e)
            return False

    def get_match_ids(self) -> any:
        try:
            collection = self.db["schedules"]
            matches = collection.find()
            match_ids = []
            for match in matches:
                match_ids.append(match["smId"])
            self.match_ids = match_ids
            return True

        except Exception as e:
            print("Exception occured while getting match history from database", e)

    def configure_match_urls(self) -> bool:
        urls = self.urls
        match_urls = {}
        for item in self.match_ids:
            match_urls[str(item) + 'in_1'] = urls.get('onScoring_ing1_p1') + \
                str(item) + urls.get('onScoring_ing1_p2')
            match_urls[str(item) + 'in_2'] = urls.get('onScoring_ing2_p1') + \
                str(item) + urls.get('onScoring_ing2_p2')
        print(match_urls)
        self.match_urls = match_urls
        return True


stats_class = DQModule()
stats_class.get_data()
