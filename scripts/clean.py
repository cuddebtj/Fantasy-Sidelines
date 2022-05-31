from api import SportRadar

# from api import MySportsFeeds
import pandas as pd
import numpy as np
import time
import gspread
import re
from gspread_pandas import Spread


def schedule_clean(data):

    schedule = pd.DataFrame()
    season = pd.json_normalize(data).add_prefix("season.")
    season_weeks = pd.json_normalize(season["season.weeks"])

    for wk in range(len(season_weeks.columns)):
        season_weeks_week = pd.json_normalize(season_weeks[wk]).add_prefix("week.")
        season_weeks_week_games = pd.json_normalize(season_weeks_week["week.games"])
        _week = pd.DataFrame()

        for gm in range(len(season_weeks_week_games.columns)):
            try:
                season_weeks_week_games_game = pd.json_normalize(
                    season_weeks_week_games[gm]
                ).add_prefix("game.")
                season_weeks_week_games_game_periods = pd.json_normalize(
                    season_weeks_week_games_game["game.scoring.periods"]
                )
                _period = pd.DataFrame()
                for p in range(len(season_weeks_week_games_game_periods.columns)):
                    season_weeks_week_games_game_periods_period = pd.json_normalize(
                        season_weeks_week_games_game_periods[p]
                    ).add_prefix(f"periods{p+1}.")
                    _period = _period.join(
                        season_weeks_week_games_game_periods_period, how="outer"
                    )

                _game = season_weeks_week_games_game.join(
                    [_period, season, season_weeks_week], how="outer"
                )

            except:
                continue

            _week = pd.concat([_game, _week])

        schedule = pd.concat([_week, schedule])

    schedule.drop(
        [
            "game.scoring.periods",
            "season._comment",
            "season.weeks",
            "week.games",
            "week.bye_week",
        ],
        axis=1,
        inplace=True,
    )
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

    schedule = schedule.rename(
        columns={
            "game.venue.id": "venue.id",
            "game.venue.name": "venue.name",
            "game.venue.city": "venue.city",
            "game.venue.state": "venue.state",
            "game.venue.country": "venue.country",
            "game.venue.zip": "venue.zip",
            "game.venue.address": "veunue.address",
            "game.venue.capacity": "venue.capacity",
            "game.venue.surface": "venue.surface",
            "game.venue.roof_type": "venue.roof_type",
            "game.venue.sr_id": "venue.sr_id",
            "game.venue.location.lat": "venue.location.lat",
            "game.venue.location.lng": "venue.location.lng",
            "game.team.id": "team.id",
            "game.team.name": "team.name",
            "game.team.alias": "team.alias",
            "game.team.game_number": "team.game_number",
            "game.team.sr_id": "team.sr_id",
            "game.opp.id": "opp.id",
            "game.opp.name": "opp.name",
            "game.opp.alias": "opp.alias",
            "game.opp.game_number": "opp.game_number",
            "game.opp.sr_id": "opp.sr_id",
        }
    )

    schedule[
        [
            "game.reference",
            "game.utc_offset",
            "game.venue.location.lat",
            "game.venue.location.lng",
            "periods1.sequence",
            "periods2.sequence",
            "periods3.sequence",
            "periods4.sequence",
            "periods5.sequence",
            "season.year",
            "week.sequence",
            "week.title",
        ]
    ] = schedule[
        [
            "game.reference",
            "game.utc_offset",
            "game.venue.location.lat",
            "game.venue.location.lng",
            "periods1.sequence",
            "periods2.sequence",
            "periods3.sequence",
            "periods4.sequence",
            "periods5.sequence",
            "season.year",
            "week.sequence",
            "week.title",
        ]
    ].astype(
        str
    )

    return schedule


def player_stats_clean(data):

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
                bridge.columns = [
                    "{}{}".format("" if z in keep_columns else c[16:-7], z)
                    for z in bridge.columns
                ]
                bridge.rename(
                    columns={
                        "id": "player.id",
                        "name": "player.name",
                        "position": "player.position",
                        "sr_id": "player.sr_id",
                        "jersey": "player.jersey_num",
                    },
                    inplace=True,
                )
                bridge[
                    [
                        "team.alias",
                        "team.name",
                        "team.id",
                        "season",
                        "season.id",
                        "week",
                        "week.id",
                        "opp.alias",
                        "opp.name",
                        "opp.id",
                        "game.id",
                    ]
                ] = team_stats[
                    [
                        "summary.home.alias",
                        "summary.home.name",
                        "summary.home.id",
                        "summary.season.year",
                        "summary.season.id",
                        "summary.week.sequence",
                        "summary.week.id",
                        "summary.away.alias",
                        "summary.away.name",
                        "summary.away.id",
                        "id",
                    ]
                ]
                bridge["home.away"] = "home"
                df_2 = pd.concat([bridge, df_2])

        elif "away" in c:
            df_1 = pd.json_normalize(player_stats[c])
            df_2 = pd.DataFrame()

            for col in df_1.columns:
                bridge = pd.json_normalize(df_1[col])
                bridge.columns = [
                    "{}{}".format("" if z in keep_columns else c[16:-7], z)
                    for z in bridge.columns
                ]
                bridge.rename(
                    columns={
                        "id": "player.id",
                        "name": "player.name",
                        "position": "player.position",
                        "sr_id": "player.sr_id",
                        "jersey": "player.jersey_num",
                    },
                    inplace=True,
                )
                bridge[
                    [
                        "team.alias",
                        "team.name",
                        "team.id",
                        "season",
                        "season.id",
                        "week",
                        "week.id",
                        "opp.alias",
                        "opp.name",
                        "opp.id",
                        "game.id",
                    ]
                ] = team_stats[
                    [
                        "summary.away.alias",
                        "summary.away.name",
                        "summary.away.id",
                        "summary.season.year",
                        "summary.season.id",
                        "summary.week.sequence",
                        "summary.week.id",
                        "summary.home.alias",
                        "summary.home.name",
                        "summary.home.id",
                        "id",
                    ]
                ]
                bridge["home.away"] = "away"
                df_2 = pd.concat([bridge, df_2])

        player_stats_df = pd.concat([df_2, player_stats_df])
        groupby_list = [
            "player.id",
            "player.name",
            "player.position",
            "player.sr_id",
            "player.jersey_num",
            "team.alias",
            "team.name",
            "team.id",
            "season",
            "season.id",
            "week",
            "week.id",
            "opp.alias",
            "opp.name",
            "opp.id",
            "game.id",
        ]

        player_stats_df = player_stats_df.groupby(groupby_list).sum().reset_index()

    replace = [" III", " II", " IV", " V", " Jr.", " Sr.", " Sr", " Jr"]
    player_stats_df["player.name"] = player_stats_df["player.name"].str.replace(
        "|".join([re.escape(s) for s in replace]), "", regex=True
    )
    player_stats_df = player_stats_df.drop_duplicates()

    player_stats_df[
        ["player.jersey_num", "season.year", "week.sequence"]
    ] = player_stats_df[["player.jersey_num", "season.year", "week.sequence"]].astype(
        str
    )

    return player_stats_df


def team_stats_clean(data):

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

    cols = []
    for col in team_stats_df.columns:
        if "game.summary." in col:
            cols.append(col[len("game.summary.") :])
        elif "game.statistics." in col:
            cols.append(col[len("game.statistics.") :])
        else:
            cols.append(col)
    team_stats_df.columns = cols

    team_stats_df[
        [
            "game.reference",
            "game.number",
            "game.utc_offset",
            "game.quarter",
            "season.year",
            "week.sequence",
            "week.title",
            "venue.location.lat",
            "venue.location.lng",
        ]
    ] = team_stats_df[
        [
            "game.reference",
            "game.number",
            "game.utc_offset",
            "game.quarter",
            "season.year",
            "week.sequence",
            "week.title",
            "venue.location.lat",
            "venue.location.lng",
        ]
    ].astype(
        str
    )

    return team_stats_df


def get_season(
    api_key,
    year,
    access_level="trial",
    version="v7",
    language_code="en",
    format_="json",
    sleep_time=2,
    timeout=5,
):

    season = SportRadar(
        api_key,
        access_level=access_level,
        version=version,
        language_code=language_code,
        format_=format_,
        sleep_time=sleep_time,
        timeout=timeout,
    )

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


def practice_par(season_start, season_end):

    gc = gspread.service_account()
    spread = gc.open("PlayerPractice")

    weeks = list(range(1, 18))
    seasons = list(range(season_start, season_end + 1))
    teams_id = [
        "Arizona Cardinals",
        "Atlanta Falcons",
        "Baltimore Ravens",
        "Buffalo Bills",
        "Carolina Panthers",
        "Chicago Bears",
        "Cincinnati Bengals",
        "Cleveland Browns",
        "Dallas Cowboys",
        "Denver Broncos",
        "Detroit Lions",
        "Green Bay Packers",
        "Houston Texans",
        "Indianapolis Colts",
        "Jacksonville Jaguars",
        "Kansas City Chiefs",
        "Los Angeles Rams",
        "San Diego Chargers",
        "Miami Dolphins",
        "Minnesota Vikings",
        "New England Patriots",
        "New Orleans Saints",
        "New York Giants",
        "New York Jets",
        "Oakland Raiders",
        "Las Vegas Raiders",
        "Philadelphia Eagles",
        "Pittsburgh Steelers",
        "Los Angeles Chargers",
        "San Francisco 49ers",
        "Seattle Seahawks",
        "Tampa Bay Buccaneers",
        "Tennessee Titans",
        "Washington Redskins",
        "Washington Football Team",
    ]

    practice_par_df = pd.DataFrame()

    for season in seasons:

        for week in weeks:

            try:
                sh = str(season)
                sheet = spread.worksheet(sh)
                data = sheet.col_values(week)

                idx = [data.index(team) for team in teams_id if team in data]
                idx.append(len(data))
                idx.sort()

                final_df = pd.DataFrame()

                for i in range(len(idx[:-1])):

                    start = idx[i] + 12
                    end = idx[i + 1]

                    final_data = {}
                    final_data["team"] = [
                        data[start - 12] for i in range(start, end, 6)
                    ]
                    final_data["player.name"] = [
                        data[i - 5] for i in range(start, end, 6)
                    ]
                    final_data["injury"] = [data[i - 4] for i in range(start, end, 6)]
                    final_data["date1"] = [
                        data[start - 9] for i in range(start, end, 6)
                    ]
                    final_data["date1.status"] = [
                        data[i - 3] for i in range(start, end, 6)
                    ]
                    final_data["date2"] = [
                        data[start - 8] for i in range(start, end, 6)
                    ]
                    final_data["date2.status"] = [
                        data[i - 2] for i in range(start, end, 6)
                    ]
                    final_data["date3"] = [
                        data[start - 7] for i in range(start, end, 6)
                    ]
                    final_data["date3.status"] = [
                        data[i - 1] for i in range(start, end, 6)
                    ]
                    final_data["game.status"] = [data[i] for i in range(start, end, 6)]

                    df = pd.DataFrame.from_dict(final_data)
                    final_df = pd.concat([final_df, df])

                final_df["season"] = season
                final_df["week"] = week

                final_df[["date1", "date2", "date3"]] = final_df[
                    ["date1", "date2", "date3"]
                ].replace(
                    {
                        "Tue ": "",
                        "Wed ": "",
                        "Thu ": "",
                        "Mon ": "",
                        "Sun ": "",
                        "Fri ": "",
                        "Sat ": "",
                    },
                    regex=True,
                )

                final_df[["last.date", "game.status", "home.away", "opp"]] = final_df[
                    "game.status"
                ].str.split(expand=True)
                final_df[["player.name", "player.pos"]] = final_df[
                    "player.name"
                ].str.split(", ", 1, expand=True)
                final_df = final_df.drop(["last.date"], axis=1)
                final_df.sort_values(["team", "player.name"], inplace=True)
                final_df["home.away"] = final_df.groupby("team")["home.away"].transform(
                    lambda x: x.bfill().ffill()
                )
                final_df["opp"] = final_df.groupby("team")["opp"].transform(
                    lambda x: x.bfill().ffill()
                )
                final_df = final_df.replace({"--": np.nan})
                final_df["game.status"] = final_df["game.status"].replace(
                    {None: "Full"}
                )
                final_df["home.away"] = final_df["home.away"].replace(
                    {"vs": "home", "@": "away"}
                )
                final_df["team"] = final_df["team"].replace(
                    {
                        "Arizona Cardinals": "ARI",
                        "Atlanta Falcons": "ATL",
                        "Baltimore Ravens": "BAL",
                        "Buffalo Bills": "BUF",
                        "Carolina Panthers": "CAR",
                        "Chicago Bears": "CHI",
                        "Cincinnati Bengals": "CIN",
                        "Cleveland Browns": "CLE",
                        "Dallas Cowboys": "DAL",
                        "Denver Broncos": "DEN",
                        "Detroit Lions": "DET",
                        "Green Bay Packers": "GB",
                        "Houston Texans": "HOU",
                        "Indianapolis Colts": "IND",
                        "Jacksonville Jaguars": "JAC",
                        "Kansas City Chiefs": "KC",
                        "Las Vegas Raiders": "LV",
                        "Los Angeles Chargers": "LAC",
                        "Los Angeles Rams": "LA",
                        "Miami Dolphins": "MIA",
                        "Minnesota Vikings": "MIN",
                        "New England Patriots": "NE",
                        "New Orleans Saints": "NO",
                        "New York Giants": "NYG",
                        "New York Jets": "NYJ",
                        "Oakland Raiders": "OAK",
                        "Philadelphia Eagles": "PHI",
                        "Pittsburgh Steelers": "PIT",
                        "San Diego Chargers": "SD",
                        "San Francisco 49ers": "SF",
                        "Seattle Seahawks": "SEA",
                        "Tampa Bay Buccaneers": "TB",
                        "Tennessee Titans": "TEN",
                        "Washington Football Team": "WAS",
                        "Washington Redskins": "WAS",
                    }
                )
                final_df["opp"] = final_df["opp"].replace(
                    {
                        "Ari": "ARI",
                        "Atl": "ATL",
                        "Bal": "BAL",
                        "Buf": "BUF",
                        "Car": "CAR",
                        "Chi": "CHI",
                        "Cin": "CIN",
                        "Cle": "CLE",
                        "Dal": "DAL",
                        "Den": "DEN",
                        "Det": "DET",
                        "Hou": "HOU",
                        "Ind": "IND",
                        "Jax": "JAC",
                        "Mia": "MIA",
                        "Min": "MIN",
                        "Oak": "OAK",
                        "Phi": "PHI",
                        "Pit": "PIT",
                        "Sea": "SEA",
                        "Ten": "TEN",
                        "Was": "WAS",
                    }
                )

                practice_par_df = pd.concat([practice_par_df, final_df])
                time.sleep(5)

            except Exception as e:
                print(e)
                print(season, week)
                break

    practice_par_df[["season", "week"]] = practice_par_df[["season", "week"]].astype(
        str
    )

    replace = [" III", " II", " IV", " V", " Jr.", " Sr.", " Sr", " Jr"]
    practice_par_df["player.name"] = practice_par_df["player.name"].str.replace(
        "|".join([re.escape(s) for s in replace]), "", regex=True
    )
    practice_par_df = practice_par_df.drop_duplicates()

    return practice_par_df


def weekly_stats(player_stats, player_snaps, player_practice):

    weekly_stats = player_stats.merge(
        player_snaps,
        how="outer",
        left_on=[
            "player.name",
            "season.year",
            "week.sequence",
            "team.alias",
            "opp.alias",
        ],
        right_on=["player_name", "season", "week", "team", "opp"],
        suffixes=(None, ".merged"),
    )

    weekly_stats["player.name"] = weekly_stats["player.name"].fillna(
        weekly_stats["player_name"]
    )
    weekly_stats["season.year"] = weekly_stats["season.year"].fillna(
        weekly_stats["season"]
    )
    weekly_stats["week.sequence"] = weekly_stats["week.sequence"].fillna(
        weekly_stats["week"]
    )
    weekly_stats["team.alias"] = weekly_stats["team.alias"].fillna(weekly_stats["team"])
    weekly_stats["opp.alias"] = weekly_stats["opp.alias"].fillna(weekly_stats["opp"])

    weekly_stats = weekly_stats.drop(
        ["player_name", "season", "week", "team", "opp"], axis=1
    )
    weekly_stats.rename(columns={"player_url": "player_address"}, inplace=True)
    weekly_stats["season.year"] = weekly_stats["season.year"].astype(str)
    weekly_stats["week.sequence"] = weekly_stats["week.sequence"].astype(str)

    weekly_stats = weekly_stats.merge(
        player_practice,
        how="outer",
        right_on=["player.name", "team", "season", "week"],
        left_on=["player.name", "team.alias", "season.year", "week.sequence"],
        suffixes=(None, ".merged"),
    )
    weekly_stats["team.alias"] = weekly_stats["team.alias"].fillna(weekly_stats["team"])
    weekly_stats["season.year"] = weekly_stats["season.year"].fillna(
        weekly_stats["season"]
    )
    weekly_stats["week.sequence"] = weekly_stats["week.sequence"].fillna(
        weekly_stats["week"]
    )
    weekly_stats["opp.alias"] = weekly_stats["opp.alias"].fillna(weekly_stats["opp"])
    weekly_stats["home.away"] = weekly_stats["home.away"].fillna(
        weekly_stats["home.away.merged"]
    )
    weekly_stats["player.position"] = weekly_stats["player.position"].fillna(
        weekly_stats["player.pos"]
    )
    weekly_stats = weekly_stats.drop(
        ["team", "season", "week", "opp", "home.away.merged", "player.pos"], axis=1
    )

    weekly_stats[
        ["player.id", "player.sr_id", "player.jersey_num", "player_address"]
    ] = weekly_stats.groupby(["player.name", "team.alias", "season.year"])[
        ["player.id", "player.sr_id", "player.jersey_num", "player_address"]
    ].apply(
        lambda x: x.ffill().bfill()
    )

    weekly_stats[["season.id"]] = weekly_stats.groupby(["season.year"])[
        ["season.id"]
    ].apply(lambda x: x.ffill().bfill())

    weekly_stats[["week.id"]] = weekly_stats.groupby(["season.id", "week.sequence"])[
        ["week.id"]
    ].apply(lambda x: x.ffill().bfill())

    weekly_stats[["team.id", "team.name"]] = weekly_stats.groupby(
        ["season.id", "team.alias"]
    )[["team.id", "team.name"]].apply(lambda x: x.ffill().bfill())

    weekly_stats[["opp.id", "opp.name"]] = weekly_stats.groupby(
        ["season.id", "opp.alias"]
    )[["opp.id", "opp.name"]].apply(lambda x: x.ffill().bfill())

    weekly_stats[["game.id"]] = weekly_stats.groupby(
        ["season.id", "week.id", "team.id"]
    )[["game.id"]].apply(lambda x: x.ffill().bfill())

    weekly_stats["date1"] = (
        weekly_stats["date1"] + "/" + weekly_stats["season.year"].astype(str)
    )
    weekly_stats["date2"] = (
        weekly_stats["date2"] + "/" + weekly_stats["season.year"].astype(str)
    )
    weekly_stats["date3"] = (
        weekly_stats["date3"] + "/" + weekly_stats["season.year"].astype(str)
    )
    weekly_stats["date1"] = pd.to_datetime(weekly_stats["date1"])
    weekly_stats["date2"] = pd.to_datetime(weekly_stats["date2"])
    weekly_stats["date3"] = pd.to_datetime(weekly_stats["date3"])
    weekly_stats.loc[weekly_stats["date1"].dt.month == 1, "date1"] = weekly_stats[
        "date1"
    ] + pd.offsets.DateOffset(years=1)
    weekly_stats.loc[weekly_stats["date2"].dt.month == 1, "date2"] = weekly_stats[
        "date2"
    ] + pd.offsets.DateOffset(years=1)
    weekly_stats.loc[weekly_stats["date3"].dt.month == 1, "date3"] = weekly_stats[
        "date3"
    ] + pd.offsets.DateOffset(years=1)

    weekly_stats[["off.snaps", "def.snaps", "st.snaps"]] = weekly_stats[
        ["off.snaps", "def.snaps", "st.snaps"]
    ].fillna(0)
    weekly_stats[["off.snaps", "def.snaps", "st.snaps"]] = weekly_stats[
        ["off.snaps", "def.snaps", "st.snaps"]
    ].replace("", 0)
    weekly_stats[["off.snaps", "def.snaps", "st.snaps"]] = weekly_stats[
        ["off.snaps", "def.snaps", "st.snaps"]
    ].astype(int)
    weekly_stats["player.jersey_num"] = weekly_stats["player.jersey_num"].astype(str)

    weekly_stats = weekly_stats[
        weekly_stats["season.year"]
        == "2016" & weekly_stats["season.year"]
        == "2017" & weekly_stats["season.year"]
        == "2018" & weekly_stats["season.year"]
        == "2019" & weekly_stats["season.year"]
        == "2020"
    ]

    return weekly_stats


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
