import json
import os
import requests
import configparser
import shutil

from tqdm import tqdm
from pprint import pprint

from pymongo import MongoClient
import pandas as pd


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
        """this function is the master controller"""
        try:
            match_details_url = self.urls['baseUrl'] + \
                self.urls['onMatchLinks']
            pprint("Updating schedules")
            match_details = self.get_match_details(match_details_url)

            if match_details:
                pprint("schedules acquired")
                pprint("Fetching schedules")
                # self.get_match_ids()

                print("match ids acquired \n configuring new urls")

                # self.configure_match_urls()
                print("urls configured \n fetching data by new url")
                # self.get_match_data_by_urls()
                # converting to dataframe
                print("Converting to dataframe")
                self.convert_to_dataframe()

        except Exception as e:
            pprint(e)

    def get_match_details(self, match_details_url) -> any:
        """this function gets match schedules from AWS"""
        try:
            results = requests.get(match_details_url, timeout=1500)
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
        """this function gets the match ids from aws"""
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
        """this function configures static urls as per the match ids"""
        urls = self.urls
        match_urls = {}
        for item in self.match_ids:
            match_urls[str(item) + 'in_1'] = urls.get('onScoring_ing1_p1') + \
                str(item) + urls.get('onScoring_ing1_p2')
            match_urls[str(item) + 'in_2'] = urls.get('onScoring_ing2_p1') + \
                str(item) + urls.get('onScoring_ing2_p2')
        self.match_urls = match_urls
        # print(match_urls)
        return True

    def get_match_data_by_urls(self) -> bool:
        """this function pulls data from aws services """
        base_url = self.urls.get("baseUrl")
        total_innings_data = []
        try:
            for key, value in tqdm(self.match_urls.items(), desc="Processing URLs"):
                complete_url = base_url + value

                results = requests.get(complete_url, timeout=1500)
                cleaned_data_str = results.content.decode(
                    "utf-8").replace("onScoring(", "").rstrip(");'")
                # print(cleaned_data_str)

                innings_data = json.loads(cleaned_data_str)
                total_innings_data.append(innings_data)

            collection = self.db[self.config['MongoDB']['innings']]
            collection.drop()
            collection.insert_many(total_innings_data)

            pprint("innings captured")
            return True
        except Exception as e:
            print("Exception occurred while getting innings data:", e)
            return False

    def convert_to_dataframe(self) -> bool:
        """this function converts the innings data present in the mongodb to different dataframes"""

        collection = self.db[self.config['MongoDB']['innings']]
        documents = collection.find()

        output_folder = "outputs"

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        else:
            shutil.rmtree(output_folder)

        batting_card_all = []
        extras_all = []
        fall_of_wickets_all = []
        wagon_wheel_all = []
        partnership_scores_all = []
        bowling_card_all = []
        manhattan_graph_all = []
        manhattan_wickets_all = []
        over_history_all = []
        wagon_wheel_summery_all = []
        batting_head_to_head_all = []
        bowling_head_to_head_all = []

        for document in documents:
            batting_card = document['Innings1']['BattingCard'] if 'Innings1' in document and 'BattingCard' in document[
                'Innings1'] else document['Innings2']['BattingCard']
            batting_card_df_s = pd.DataFrame(batting_card)
            batting_card_all.append(batting_card_df_s)

            extras = document['Innings1']['Extras'] if 'Innings1' in document and 'Extras' in document['Innings1'] else document['Innings2']['Extras']
            extras_df_s = pd.DataFrame(extras)
            extras_all.append(extras_df_s)

            fall_of_wickets = document['Innings1']['FallOfWickets'] if 'Innings1' in document and 'FallOfWickets' in document[
                'Innings1'] else document['Innings2']['FallOfWickets']
            fall_of_wickets_df_s = pd.DataFrame(fall_of_wickets)
            fall_of_wickets_all.append(fall_of_wickets_df_s)

            wagon_wheel = document['Innings1']['WagonWheel'] if 'Innings1' in document and 'WagonWheel' in document[
                'Innings1'] else document['Innings2']['WagonWheel']
            wagon_wheel_df_s = pd.DataFrame(wagon_wheel)
            wagon_wheel_all.append(wagon_wheel_df_s)

            partnership_scores = document['Innings1']['PartnershipScores'] if 'Innings1' in document and 'PartnershipScores' in document[
                'Innings1'] else document['Innings2']['PartnershipScores']
            partnership_scores_df_s = pd.DataFrame(partnership_scores)
            partnership_scores_all.append(partnership_scores_df_s)

            bowling_card = document['Innings1']['BowlingCard'] if 'Innings1' in document and 'BowlingCard' in document[
                'Innings1'] else document['Innings2']['BowlingCard']
            bowling_card_df_s = pd.DataFrame(bowling_card)
            bowling_card_all.append(bowling_card_df_s)

            manhattan_graph = document['Innings1']['ManhattanGraph'] if 'Innings1' in document and 'ManhattanGraph' in document[
                'Innings1'] else document['Innings2']['ManhattanGraph']
            manhattan_graph_df_s = pd.DataFrame(manhattan_graph)
            manhattan_graph_all.append(manhattan_graph_df_s)

            manhattan_wickets = document['Innings1']['ManhattanWickets'] if 'Innings1' in document and 'ManhattanWickets' in document[
                'Innings1'] else document['Innings2']['ManhattanWickets']
            manhattan_wickets_df_s = pd.DataFrame(manhattan_wickets)
            manhattan_wickets_all.append(manhattan_wickets_df_s)

            over_history = document['Innings1']['OverHistory'] if 'Innings1' in document and 'OverHistory' in document[
                'Innings1'] else document['Innings2']['OverHistory']
            over_history_df_s = pd.DataFrame(over_history)
            over_history_all.append(over_history_df_s)

            wagon_wheel_summery = document['Innings1']['WagonWheelSummary'] if 'Innings1' in document and 'WagonWheelSummary' in document[
                'Innings1'] else document['Innings2']['WagonWheelSummary']
            wagon_wheel_summery_df_s = pd.DataFrame(wagon_wheel_summery)
            wagon_wheel_summery_all.append(wagon_wheel_summery_df_s)

            batting_head_to_head = document['Innings1']['battingheadtohead'] if 'Innings1' in document and 'battingheadtohead' in document[
                'Innings1'] else document['Innings2']['battingheadtohead']
            batting_head_to_head_df_s = pd.DataFrame(batting_head_to_head)
            batting_head_to_head_all.append(batting_head_to_head_df_s)

            bowling_head_to_head = document['Innings1']['bowlingheadtohead'] if 'Innings1' in document and 'bowlingheadtohead' in document[
                'Innings1'] else document['Innings2']['bowlingheadtohead']
            bowling_head_to_head_df_s = pd.DataFrame(bowling_head_to_head)
            bowling_head_to_head_all.append(bowling_head_to_head_df_s)

        batting_card_df = pd.concat(batting_card_all, ignore_index=True)
        extras_df = pd.concat(extras_all, ignore_index=True)
        fall_of_wickets_df = pd.concat(fall_of_wickets_all, ignore_index=True)
        wagon_wheel_df = pd.concat(wagon_wheel_all, ignore_index=True)
        partnership_scores_df = pd.concat(
            partnership_scores_all, ignore_index=True)
        bowling_card_df = pd.concat(bowling_card_all, ignore_index=True)
        manhattan_graph_df = pd.concat(manhattan_graph_all, ignore_index=True)
        manhattan_wickets_df = pd.concat(
            manhattan_wickets_all, ignore_index=True)
        over_history_df = pd.concat(over_history_all, ignore_index=True)
        wagon_wheel_summery_df = pd.concat(wagon_wheel_all, ignore_index=True)
        batting_head_to_head_df = pd.concat(
            batting_head_to_head_all, ignore_index=True)
        bowling_head_to_head_df = pd.concat(
            bowling_head_to_head_all, ignore_index=True)

        batting_card_df.to_csv(os.path.join(
            output_folder, "batting_card.csv"), index=False)
        extras_df.to_csv(os.path.join(
            output_folder, "extras.csv"), index=False)
        fall_of_wickets_df.to_csv(os.path.join(
            output_folder, "fall_of_wickets.csv"), index=False)
        wagon_wheel_df.to_csv(os.path.join(
            output_folder, "wagon_wheel.csv"), index=False)
        partnership_scores_df.to_csv(os.path.join(
            output_folder, "partnership_scores.csv"), index=False)
        bowling_card_df.to_csv(os.path.join(
            output_folder, "bowling_card.csv"), index=False)
        manhattan_graph_df.to_csv(os.path.join(
            output_folder, "manhattan_graph.csv"), index=False)
        manhattan_wickets_df.to_csv(os.path.join(
            output_folder, "manhattan_wickets.csv"), index=False)
        over_history_df.to_csv(os.path.join(
            output_folder, "over_history.csv"), index=False)
        wagon_wheel_summery_df.to_csv(os.path.join(
            output_folder, "wagon_wheel_summery.csv"), index=False)
        batting_head_to_head_df.to_csv(os.path.join(
            output_folder, "batting_head_to_head.csv"), index=False)
        bowling_head_to_head_df.to_csv(os.path.join(
            output_folder, "bowling_head_to_head.csv"), index=False)

        return True


stats_class = DQModule()
stats_class.get_data()
