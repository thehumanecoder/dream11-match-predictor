import json
import os
import configparser
import shutil

from pprint import pprint
from multiprocessing.pool import ThreadPool

import requests
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
from termcolor import colored


class DQModule:
    def __init__(self) -> None:

        print(colored(
            "\n This is Data Query module built to download Indian Premere League data into datasets", "green", attrs=["bold"]))
        print(colored(" \n Credits: The Humane Coder ",
              "red", attrs=["blink"]))

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
        cleaned_data_str = response.content.decode(
            "utf-8").replace("onMatchLinks(", "").rstrip(");'")
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

    def get_match_data_by_urls(self) -> bool:
        """This function pulls data from AWS services and stores in MongoDB."""
        base_url = self.urls.get("baseUrl")
        total_innings_data = []
        session = requests.Session()  # Reuse the same session for better performance

        try:
            for key, value in tqdm(self.match_urls.items(), desc="Processing URLs"):
                # More efficient string concatenation
                complete_url = f"{base_url}{value}"

                try:
                    response = session.get(complete_url, timeout=1500)
                    response.raise_for_status()  # Raise exception for bad responses (4xx, 5xx)

                    cleaned_data_str = response.content.decode(
                        "utf-8").replace("onScoring(", "").rstrip(");'")

                    innings_data = json.loads(cleaned_data_str)
                    total_innings_data.append(innings_data)

                except requests.exceptions.HTTPError as http_err:
                    if response.status_code == 404:
                        print(f"404 Error: URL {complete_url} not found.")
                    else:
                        print(f"HTTP error occurred: {http_err}")
                except requests.exceptions.RequestException as req_err:
                    print(f"Request error occurred: {req_err}")
                except json.JSONDecodeError as json_err:
                    print(
                        f"JSON decoding error for URL {complete_url}: {json_err}")

            if total_innings_data:
                collection = self.db[self.config['MongoDB']['innings']]
                # Clear the collection instead of dropping it
                collection.delete_many({})
                collection.insert_many(total_innings_data)  # Batch insert

                pprint("Innings data captured successfully.")
                return True
            else:
                print("No valid innings data to insert.")
                return False

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return False

        finally:
            session.close()

    def convert_to_dataframe(self) -> bool:
        """This function converts the innings data present in the MongoDB to different dataframes."""
        collection = self.db[self.config['MongoDB']['innings']]
        documents = collection.find()

        output_folder = "outputs"

        # Efficient folder handling: Only delete the folder if it exists and contains files
        if os.path.exists(output_folder):
            if len(os.listdir(output_folder)) > 0:
                shutil.rmtree(output_folder)
        os.makedirs(output_folder, exist_ok=True)

        # Initialize lists to store extracted data
        data_dict = {
            'batting_card_all': [],
            'extras_all': [],
            'fall_of_wickets_all': [],
            'wagon_wheel_all': [],
            'partnership_scores_all': [],
            'bowling_card_all': [],
            'manhattan_graph_all': [],
            'manhattan_wickets_all': [],
            'over_history_all': [],
            'wagon_wheel_summery_all': [],
            'batting_head_to_head_all': [],
            'bowling_head_to_head_all': []
        }

        # Process the documents
        for document in documents:
            innings = document.get('Innings1') or document.get('Innings2')

            if innings:
                self._append_data(
                    data_dict['batting_card_all'], innings, 'BattingCard')
                self._append_data(data_dict['extras_all'], innings, 'Extras')
                self._append_data(
                    data_dict['fall_of_wickets_all'], innings, 'FallOfWickets')
                self._append_data(
                    data_dict['wagon_wheel_all'], innings, 'WagonWheel')
                self._append_data(
                    data_dict['partnership_scores_all'], innings, 'PartnershipScores')
                self._append_data(
                    data_dict['bowling_card_all'], innings, 'BowlingCard')
                self._append_data(
                    data_dict['manhattan_graph_all'], innings, 'ManhattanGraph')
                self._append_data(
                    data_dict['manhattan_wickets_all'], innings, 'ManhattanWickets')
                self._append_data(
                    data_dict['over_history_all'], innings, 'OverHistory')
                self._append_data(
                    data_dict['wagon_wheel_summery_all'], innings, 'WagonWheelSummary')
                self._append_data(
                    data_dict['batting_head_to_head_all'], innings, 'battingheadtohead')
                self._append_data(
                    data_dict['bowling_head_to_head_all'], innings, 'bowlingheadtohead')

        # Concatenate the data lists into DataFrames
        df_dict = {key: pd.concat(data_list, ignore_index=True)
                   for key, data_list in data_dict.items()}

        # List of filenames corresponding to the data
        file_names = [
            "batting_card.csv", "extras.csv", "fall_of_wickets.csv", "wagon_wheel.csv", "partnership_scores.csv",
            "bowling_card.csv", "manhattan_graph.csv", "manhattan_wickets.csv", "over_history.csv",
            "wagon_wheel_summery.csv", "batting_head_to_head.csv", "bowling_head_to_head.csv"
        ]

        # Multi-threaded file writing for performance optimization
        with ThreadPool() as pool:
            pool.starmap(self._write_csv, [(df_dict[key], os.path.join(output_folder, file_names[i]))
                                           for i, key in enumerate(df_dict)])
        print("Process Complete")
        return True

    def _append_data(self, data_list, innings, key):
        """Helper function to append data to the corresponding list."""
        data = innings.get(key, [])
        if data:  # Only append if data exists
            data_list.append(pd.DataFrame(data))

    def _write_csv(self, df, filepath):
        """Helper function to write a DataFrame to CSV."""
        df.to_csv(filepath, index=False)
        print(f"{filepath} : successfully converted to dataset")
        return True


stats_class = DQModule()
stats_class.get_data()
