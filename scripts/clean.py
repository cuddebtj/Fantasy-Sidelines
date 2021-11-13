from api import SportRadar
# from api import MySportsFeeds
import pandas as pd
import time


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


def team_stats_clean(self, data):
    
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
    
    
def get_season(api_key, year, access_level="trial", version="v7", language_code="en", format_="json", sleep_time=2, timeout=5):
    
    season = SportRadar(api_key, access_level=access_level, version=version, language_code=language_code, format_=format_, sleep_time=sleep_time, timeout=timeout)

    schedule_api = season.season_schedule(year)
    schedule = schedule_clean(schedule_api)
    game_id_list = list(schedule["game.id"].unique())
    
    players_stats = pd.DataFrame()
    teams_stats = pd.DataFrame()

    for game_id in range(len(game_id_list)):
        statistics_api = season.game_statistics(game_id_list[game_id])
        
        players = player_stats_clean(statistics_api)
        teams = team_stats_clean(statistics_api)
        
        players_stats = pd.concat([players, players_stats])
        teams_stats = pd.concat([teams, teams_stats])

    return schedule, players_stats, teams_stats


# def all_snaps(api_key, seasons, weeks):
# 
#     api = MySportsFeeds(api_key)
#     start = time.time()
#     final_df = pd.DataFrame()
#     for s in seasons:
#         for w in weeks:
#             snaps = api.snaps_api(s, w)
#             final_df = pd.concat([snaps, final_df])
            
#     print(time.time()-start)
# 
#     return final_df