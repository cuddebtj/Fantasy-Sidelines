import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
import base64
import time


class SportRadar:
    def __init__(
        self,
        api_key,
        access_level="trial",
        version="v7",
        language_code="en",
        format_="json",
        sleep_time=2,
        timeout=5,
    ):
        self.session = requests.Session()
        self.api_key = api_key
        self.access_level = access_level
        self.version = version
        self.language_code = language_code
        self.format_ = format_
        self._sleep_time = sleep_time
        self.timeout = timeout
        self.api_root = f"https://api.sportradar.us/nfl/official/{self.access_level}/{self.version}/{self.language_code}/"

    def season_schedule(self, year, nfl_season="reg"):
        year = year
        api_path = f"{self.api_root}games/{str(year)}/{nfl_season}/schedule.{self.format_}?api_key={self.api_key}"
        time.sleep(self._sleep_time)

        try:
            data = self.session.get(api_path, timeout=self.timeout)
            data.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)

        return data.json()

    def game_statistics(self, game_id):
        api_path = f"{self.api_root}games/{str(game_id)}/statistics.{self.format_}?api_key={self.api_key}"
        time.sleep(self._sleep_time)

        try:
            data = self.session.get(api_path, timeout=self.timeout)
            data.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)

        return data.json()


#     def game_roster(self, game_id):
#         api_path = f"{self.api_root}games/{str(team_id)}/profile.{self._format}?api_key={self.api_key}"
#         time.sleep(self._sleep_time)

#         try:
#             data = self.session.get(api_path, timeout=self.timeout)
#             data.raise_for_status()
#         except requests.exceptions.HTTPError as errh:
#             print ("Http Error:",errh)
#         except requests.exceptions.ConnectionError as errc:
#             print ("Error Connecting:",errc)
#         except requests.exceptions.Timeout as errt:
#             print ("Timeout Error:",errt)
#         except requests.exceptions.RequestException as err:
#             print ("OOps: Something Else",err)

#         return data.json()


# class MySportsFeeds():


#     def __init__(self, api_key):
#         self.api_key = api_key

#     def snaps_api(self, season, week):
#     api_url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{season[0]}/week/{week[0]}/player_gamelogs.json"

#     try:
#         response = requests.get(
#             url=f"{api_url}",
#             params={
#                 "fordate": "20211103"
#             },
#             headers={
#                 "Authorization": "Basic " + base64.b64encode("{}:{}".format(self.api_key, "MYSPORTSFEEDS").encode("utf-8")).decode("ascii")
#             }
#         )
#         print("Response HTTP Status Code: {status_code}".format(
#             status_code=response.status_code))
#     except requests.exceptions.RequestException:
#         print("HTTP Request failed")

#     r = response.content
#     soup = BeautifulSoup(r, "html.parser")
#     json = soup.text
#     df = pd.json_normalize(json["gamelogs"])
#     cols = list(df.columns)
#     not_drop = []
#     for c in cols:
#         if "game." in c or "player." in c or "team." in c or "snapCounts" in c:
#             not_drop.append(c)

#     snaps = df[not_drop]

#     time.sleep(5)

#     return snaps
