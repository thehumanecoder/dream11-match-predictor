import json
import os
import configparser
import shutil
import logging

import requests
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd

class DQModule:
    def __init__(self) -> None:
        self.load_config()
        self.mongo_client = MongoClient(self.mongo_url)
        self.db = self.mongo_client[self.mongo_client_name]
        self.match_ids = []
        self.match_urls = {}

    def load_config(self) -> None:
        url_file_path = os.path.join('configs', 'urls.json')
        config_file_path = os.path.join('configs', 'config.ini')

        with open(url_file_path, 'r', encoding='utf-8') as file:
            self.urls = json.load(file)

        self.config = configparser.ConfigParser()
        self.config.read(config_file_path)

        self.mongo_url = self.config['MongoDB']['url']
        self.mongo_client_name = self.config['MongoDB']['client']

    def get_data(self) -> None:
        try:
            self.update_schedules()
            self.get_match_ids()
            self.configure_match_urls()
            self.fetch_data_by_urls()
            self.convert_to_dataframe()
        except Exception as e:
            logging.error(f"Error in get_data: {e}")

    def update_schedules(self) -> None:
        try:
            match_details_url = f"{self.urls['baseUrl']}{self.urls['onMatchLinks']}"
            logging.info("Updating schedules")
            if self.fetch_and_store_schedules(match_details_url):
                logging.info("Schedules updated successfully")
        except requests.RequestException as e:
            logging.error(f"Error fetching match schedules: {e}")

    def fetch_and_store_schedules(self, url: str) -> bool:
        response = requests.get(url, timeout=1500)
        cleaned_data_str = response.content.decode("utf-8").replace("onMatchLinks(", "").rstrip(");'")
        match_schedules = json.loads(cleaned_data_str)

        collection = self.db[self.config['MongoDB']['schedules']]
        collection.drop()
        collection.insert_many(match_schedules)
        return True

    def get_match_ids(self) -> None:
        try:
            collection = self.db["schedules"]
            self.match_ids = [match["smId"] for match in collection.find()]
        except Exception as e:
            logging.error(f"Error getting match IDs: {e}")

    def configure_match_urls(self) -> None:
        self.match_urls = {}
        for item in self.match_ids:
            self.match_urls[f"{item}in_1"] = f"{self.urls['onScoring_ing1_p1']}{item}{self.urls['onScoring_ing1_p2']}"
            self.match_urls[f"{item}in_2"] = f"{self.urls['onScoring_ing2_p1']}{item}{self.urls['onScoring_ing2_p2']}"

    def fetch_data_by_urls(self) -> None:
        total_innings_data = []

        try:
            for key, value in tqdm(self.match_urls.items(), desc="Processing URLs"):
                complete_url = f"{self.urls['baseUrl']}{value}"
                response = requests.get(complete_url, timeout=15000)
                cleaned_data_str = response.content.decode("utf-8").replace("onScoring(", "").rstrip(");'")
                innings_data = json.loads(cleaned_data_str)
                total_innings_data.append(innings_data)

            collection = self.db[self.config['MongoDB']['innings']]
            collection.drop()
            collection.insert_many(total_innings_data)
            logging.info("Innings data fetched and stored successfully")
        except requests.RequestException as e:
            logging.error(f"Error fetching innings data: {e}")

    def convert_to_dataframe(self) -> None:
        try:
            collection = self.db[self.config['MongoDB']['innings']]
            documents = collection.find()
            self.save_dataframes(documents)
        except Exception as e:
            logging.error(f"Error converting to DataFrame: {e}")

    def save_dataframes(self, documents) -> None:
        output_folder = "outputs"
        os.makedirs(output_folder, exist_ok=True)

        data_dict = {
            "batting_card": [],
            "extras": [],
            "fall_of_wickets": [],
            "wagon_wheel": [],
            "partnership_scores": [],
            "bowling_card": [],
            "manhattan_graph": [],
            "manhattan_wickets": [],
            "over_history": [],
            "wagon_wheel_summery": [],
            "batting_head_to_head": [],
            "bowling_head_to_head": []
        }

        for document in documents:
            self.extract_document_data(document, data_dict)

        for key, data in data_dict.items():
            if data:
                combined_df = pd.concat(data, ignore_index=True)
                combined_df.to_csv(os.path.join(output_folder, f"{key}.csv"), index=False)

    def extract_document_data(self, document, data_dict) -> None:
        for key in data_dict.keys():
            if 'Innings1' in document and key in document['Innings1']:
                df = pd.DataFrame(document['Innings1'][key])
                data_dict[key].append(df)
            elif 'Innings2' in document and key in document['Innings2']:
                df = pd.DataFrame(document['Innings2'][key])
                data_dict[key].append(df)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dq_module = DQModule()
    dq_module.get_data()
