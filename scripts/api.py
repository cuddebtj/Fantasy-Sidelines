# -*- coding: utf-8 -*-

"""
{Description}

MIT License

Copyright (c) 2021, Fantasy-Sidelines

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

API details and documentation: https://developer.sportradar.com/docs
"""

import requests
import time
import pandas as pd

__author__ = "Tim Cuddeback"
__copyright__ = "Copyright (c) 2021, Fantasy-Sidelines"
__credits__ = ["Tim Cuddeback", "Sportradar API", "Fantasy Sharks"]
__license__ = "MIT License"
__version__ = "1.0.0"
__maintainer__ = "Tim Cuddeback"
__email__ = "cuddebtj@gmail.com"
__status__ = "Dev"



class NFL():
    
    
    def __init__(self, api_key, access_level="trial", version="v7", language_code="en", format_="json", sleep_time=2, timeout=5):
        self.session = requests.Session()
        self.api_key = api_key
        self.access_level = access_level
        self.version = version
        self.language_code = language_code
        self.format_ = format_
        self._sleep_time = sleep_time
        self.timeout = timeout
        self.api_root = f"https://api.sportradar.us/nfl/official/{self.access_level}/{self.version}/{self.language_code}/games/"
        
    def season_schedule(self, year, nfl_season="reg"):
        year = year
        api_path = f"{self.api_root}{str(year)}/{nfl_season}/schedule.{self.format_}?api_key={self.api_key}"
        time.sleep(self._sleep_time)
        data = self.session.get(api_path, timeout=self.timeout)
        
        data.raise_for_status()
        
        if data.status_code != 204:
            return data.json()
        
        return data
    
    def game_statistics(self, game_id):
        api_path = f"{self.api_root}{str(game_id)}/statistics.{self.format_}?api_key={self.api_key}"
        time.sleep(self._sleep_time)
        data = self.session.get(api_path, timeout=self.timeout)
        
        data.raise_for_status()
        
        if data.status_code != 204:
            return data.json()
        
        return data

    
    
class CLEAN():
    
    
    def __init__(self):
        pass
        
    def schedule_clean(self, data):
        schedule = pd.DataFrame()
        season = pd.json_normalize(data).add_prefix("season.")
        season_weeks = pd.json_normalize(season["season.weeks"])

        for wk in range(len(season_weeks.columns)):
            season_weeks_week = pd.json_normalize(season_weeks[wk]).add_prefix("week.")
            season_weeks_week_games = pd.json_normalize(season_weeks_week["week.games"])
            _week = pd.DataFrame()

            for gm in range(len(season_weeks_week_games.columns)):
                try:
                    season_weeks_week_games_game = pd.json_normalize(season_weeks_week_games[gm]).add_prefix("game.")
                    season_weeks_week_games_game_periods = pd.json_normalize(season_weeks_week_games_game["game.scoring.periods"])
                    _period = pd.DataFrame()
                    for p in range(len(season_weeks_week_games_game_periods.columns)):
                        season_weeks_week_games_game_periods_period = pd.json_normalize(season_weeks_week_games_game_periods[p]).add_prefix(f"periods{p+1}.")
                        _period = _period.join(season_weeks_week_games_game_periods_period, how="outer")

                    _game = season_weeks_week_games_game.join([_period, season, season_weeks_week], how="outer")

                except:
                    continue

                _week = pd.concat([_game, _week])

            schedule = pd.concat([_week, schedule])

        schedule.drop(["game.scoring.periods", "season._comment", "season.weeks", "week.games", "week.bye_week"], axis=1, inplace=True)
        away = schedule.copy(deep=True)
        home = schedule.copy(deep=True)
        for col in home.columns:
            home = home.rename(columns={col: str(col).replace("away", "opp")})
        for col in home.columns:
            home = home.rename(columns={col: str(col).replace("home", "team")})
        home["home.away"] = "home"

        for col in away.columns:
            away = away.rename(columns={col: str(col).replace("home", "opp")})
        for col in away.columns:
            away = away.rename(columns={col: str(col).replace("away", "team")})
        away["home.away"] = "away"
        schedule = pd.concat([home, away])
        
        return schedule
    
    def player_stats_clean(self, data):
        df = pd.json_normalize(data)
        
        team_cols = []
        players_cols = []
        for col in df.columns:
            if "player" not in col:
                team_cols.append(col)
            elif "player" in col:
                players_cols.append(col)

        team_stats = df[team_cols]
        player_stats = df[players_cols]
        
        player_stats_df = pd.DataFrame()
        
        for c in players_cols:

            keep_columns = ["id", "name", "position", "sr_id", "jersey"]

            if "home" in c:
                df_1 = pd.json_normalize(player_stats[c])
                df_2 = pd.DataFrame()
                keep_columns = ["id", "name", "position", "sr_id", "jersey"]

                for col in df_1.columns:
                        bridge = pd.json_normalize(df_1[col])
                        bridge.columns = ["{}{}".format('' if z in keep_columns else c[16:-7], z) for z in bridge.columns]
                        bridge.rename(columns={"id": "player.id", 
                                               "name": "player.name", 
                                               "position": "player.position", 
                                               "sr_id": "player.sr_id", 
                                               "jersey": "player.jersey_num"}, inplace=True)
                        bridge[["team.alias", 
                                "team.name", 
                                "team.id", 
                                "season", 
                                "season.id", 
                                "week", 
                                "week.id", 
                                "opp.alias", 
                                "opp.name", 
                                "opp.id", 
                                "game.id"]] = team_stats[["summary.home.alias",
                                                          "summary.home.name", 
                                                          "summary.home.id", 
                                                          "summary.season.year", 
                                                          "summary.season.id", 
                                                          "summary.week.sequence", 
                                                          "summary.week.id", 
                                                          "summary.away.alias", 
                                                          "summary.away.name", 
                                                          "summary.away.id", 
                                                          "id"]]
                        bridge["home.away"] = "home"
                        df_2 = pd.concat([bridge, df_2])

            elif "away" in c:
                df_1 = pd.json_normalize(player_stats[c])
                df_2 = pd.DataFrame()

                for col in df_1.columns:
                        bridge = pd.json_normalize(df_1[col])
                        bridge.columns = ["{}{}".format('' if z in keep_columns else c[16:-7], z) for z in bridge.columns]
                        bridge.rename(columns={"id": "player.id", 
                                               "name": "player.name", 
                                               "position": "player.position", 
                                               "sr_id": "player.sr_id", 
                                               "jersey": "player.jersey_num"}, inplace=True)
                        bridge[["team.alias", 
                                "team.name", 
                                "team.id", 
                                "season", 
                                "season.id", 
                                "week", 
                                "week.id", 
                                "opp.alias", 
                                "opp.name", 
                                "opp.id", 
                                "game.id"]] = team_stats[["summary.away.alias",  
                                                          "summary.away.name", 
                                                          "summary.away.id", 
                                                          "summary.season.year", 
                                                          "summary.season.id", 
                                                          "summary.week.sequence", 
                                                          "summary.week.id", 
                                                          "summary.home.alias", 
                                                          "summary.home.name", 
                                                          "summary.home.id", 
                                                          "id"]]
                        bridge["home.away"] = "away"
                        df_2 = pd.concat([bridge, df_2])

            player_stats_df = pd.concat([df_2, player_stats_df])
            groupby_list = ["player.id", "player.name", "player.position", 
                            "player.sr_id", "player.jersey_num", "team.alias", 
                            "team.name", "team.id", "season", "season.id", 
                            "week", "week.id", "opp.alias", "opp.name", 
                            "opp.id", "game.id"]

            player_stats_df = player_stats_df.groupby(groupby_list).sum().reset_index()
    
        return player_stats_df
        
    def teams_stats_clean(self, data):
        df = pd.json_normalize(data)
        
        team_cols = []
        for col in df.columns:
            if "player" not in col and "_comment" not in col:
                team_cols.append(col)
                
        team_stats_df = df[team_cols]
        team_stats_df = team_stats_df.add_prefix("game.")
        
        away = team_stats_df.copy(deep=True)
        home = team_stats_df.copy(deep=True)
        
        for col in home.columns:
            home = home.rename(columns={col: str(col).replace("away", "opp")})
        for col in home.columns:
            home = home.rename(columns={col: str(col).replace("home", "team")})
        home["home.away"] = "home"

        for col in away.columns:
            away = away.rename(columns={col: str(col).replace("home", "opp")})
        for col in away.columns:
            away = away.rename(columns={col: str(col).replace("away", "team")})
        away["home.away"] = "away"
        
        team_stats_df = pd.concat([home, away])
        
        return team_stats_df