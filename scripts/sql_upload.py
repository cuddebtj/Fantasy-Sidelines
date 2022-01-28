import pandas as pd
import time
from scrape import *
from clean import get_season, practice_par


def schedule_stats_api_sql(api_key, start_year, end_year, engine):

    schedule_final = pd.DataFrame()
    players_stats_final = pd.DataFrame()
    teams_stats_final = pd.DataFrame()
    
    try:
        for year in range(start_year, end_year+1):
            
            schedule, players_stats, teams_stats = get_season(api_key, year)

            schedule_final = pd.concat([schedule_final, schedule])
            players_stats_final = pd.concat([players_stats_final, players_stats])
            teams_stats_final = pd.concat([teams_stats_final, teams_stats])

        schedule_final.to_sql("schedule", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
        players_stats_final.to_sql("playerStats", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
        teams_stats_final.to_sql("teamStats", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
        
        print("Done. Upload to SQL successful.")
            
    except Exception as e:
        
        try:
            schedule_final.to_sql("schedule", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
            players_stats_final.to_sql("playerStats", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
            teams_stats_final.to_sql("teamStats", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
            
            print("Error: ", e, "Uploaded current data to SQL",sep="\n")
            
        except Exception as e_:
            print("Error: ", e_, sep="\n")

# schedule_2020, players_stats_2020, teams_stats_2020 = get_season(api_key[0], year[0])  
# schedule_2019, players_stats_2019, teams_stats_2019 = get_season(api_key[0], year[1])
# schedule_2018, players_stats_2018, teams_stats_2018 = get_season(api_key[0], year[2])
# schedule_2017, players_stats_2017, teams_stats_2017 = get_season(api_key[2], year[3])
# schedule_2016, players_stats_2016, teams_stats_2016 = get_season(api_key[1], year[4])
# schedule_2015, players_stats_2015, teams_stats_2015 = get_season(api_key[1], year[5])

# schedule = pd.concat([schedule_2020, schedule_2019, schedule_2018, schedule_2017, schedule_2016, schedule_2015])
# players_stats = pd.concat([players_stats_2020, players_stats_2019, players_stats_2018, players_stats_2017, players_stats_2016, players_stats_2015])
# teams_stats = pd.concat([teams_stats_2020, teams_stats_2019, teams_stats_2018, teams_stats_2017, teams_stats_2016, teams_stats_2015])


def snaps(y1, y2, engine):
    
    try:
        players_df = player_scrape()
        snaps_df = snap_scrape(y1, y2, players_df)
        snaps_df.to_sql("playerSnaps", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
    except Exception as e:
        print("Error: "+e)


def injuries(y1, y2, engine):
    
    injuries_df = pd.DataFrame()
    
    for season in range(y1, y2+1):
        
        injury = injury_scrape(season)
        injuries_df = pd.concat([injuries_df, injury])
    
    injuries_df.to_sql("playerInjuires", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)


def week_stats(weekly_stats, engine):
    
    weekly_stats.to_sql("weeklyStats", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)


def player_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.playerStats", engine)

        player_table_df = table[["player.id", "player.name", "player.position", "player.sr_id", "player.jersey_num", "team.alias", "team.name", "team.id"]]
        player_table_df = player_table_df.groupby(["player.id", 
                                                   "player.name", 
                                                   "player.sr_id"])[["player.position", 
                                                                     "player.jersey_num",
                                                                     "team.alias",
                                                                     "team.name",
                                                                     "team.id"]].agg(["unique"])
        player_table_df = player_table_df.reset_index()
        player_table_df.columns = player_table_df.columns.droplevel(1)
        player_table_df["player.position"] = player_table_df["player.position"].apply(lambda x: ", ".join(map(str, x)))
        player_table_df["player.jersey_num"] = player_table_df["player.jersey_num"].apply(lambda x: ", ".join(map(str, x)))
        player_table_df["team.alias"] = player_table_df["team.alias"].apply(lambda x: ", ".join(map(str, x)))
        player_table_df["team.name"] = player_table_df["team.name"].apply(lambda x: ", ".join(map(str, x)))
        player_table_df["team.id"] = player_table_df["team.id"].apply(lambda x: ", ".join(map(str, x)))


        player_table_df.to_sql("IDPlayerTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
       
    
def game_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)

        game_id_df = table[["game.id", "game.status", "game.reference", "game.number", "game.scheduled",
                            "game.attendance", "game.utc_offset", "game.entry_mode", "game.sr_id", "game.broadcast.internet", 
                            "game.broadcast.network", "venue.id", "venue.name", "venue.city", "venue.state", 
                            "venue.country", "venue.zip", "venue.address", "venue.capacity", "venue.surface", 
                            "venue.roof_type", "venue.sr_id", "venue.location.lat", "venue.location.lng",
                            "team.id", "team.name", "team.alias", "team.game_number", "team.sr_id", 
                            "opp.id", "opp.name", "opp.alias", "opp.game_number", "opp.sr_id",
                            "season.id", "season.year", "season.type", "season.name", "week.id", "week.sequence", "week.title", "home.away"]]
        game_id_df = game_id_df[game_id_df["home.away"] == "home"]
        game_id_df = game_id_df.drop("home.away", axis=1)
        game_id_df = game_id_df.groupby("game.id").agg(["unique"])
        game_id_df = game_id_df.reset_index()
        game_id_df.columns = game_id_df.columns.droplevel(1)
        game_id_df["game.status"] = game_id_df["game.status"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.reference"] = game_id_df["game.reference"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.number"] = game_id_df["game.number"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.scheduled"] = game_id_df["game.scheduled"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.attendance"] = game_id_df["game.attendance"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.utc_offset"] = game_id_df["game.utc_offset"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.entry_mode"] = game_id_df["game.entry_mode"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.sr_id"] = game_id_df["game.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.broadcast.internet"] = game_id_df["game.broadcast.internet"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["game.broadcast.network"] = game_id_df["game.broadcast.network"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.id"] = game_id_df["venue.id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.name"] = game_id_df["venue.name"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.city"] = game_id_df["venue.city"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.state"] = game_id_df["venue.state"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.country"] = game_id_df["venue.country"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.zip"] = game_id_df["venue.zip"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.address"] = game_id_df["venue.address"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.capacity"] = game_id_df["venue.capacity"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.surface"] = game_id_df["venue.surface"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.roof_type"] = game_id_df["venue.roof_type"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.sr_id"] = game_id_df["venue.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.location.lat"] = game_id_df["venue.location.lat"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["venue.location.lng"] = game_id_df["venue.location.lng"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["team.id"] = game_id_df["team.id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["team.name"] = game_id_df["team.name"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["team.alias"] = game_id_df["team.alias"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["team.game_number"] = game_id_df["team.game_number"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["team.sr_id"] = game_id_df["team.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["opp.id"] = game_id_df["opp.id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["opp.name"] = game_id_df["opp.name"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["opp.alias"] = game_id_df["opp.alias"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["opp.game_number"] = game_id_df["opp.game_number"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["opp.sr_id"] = game_id_df["opp.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["season.id"] = game_id_df["season.id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["season.year"] = game_id_df["season.year"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["season.name"] = game_id_df["season.name"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["season.type"] = game_id_df["season.type"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["week.id"] = game_id_df["week.id"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["week.sequence"] = game_id_df["week.sequence"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df["week.title"] = game_id_df["week.title"].apply(lambda x: ", ".join(map(str, x)))
        game_id_df[
            ["game.number",
            "game.attendance",
            "venue.capacity",
            "team.game_number",
            "opp.game_number"]
        ] = game_id_df[
            ["game.number",
            "game.attendance",
            "venue.capacity",
            "team.game_number",
            "opp.game_number"]
        ].astype(int)

        game_id_df.to_sql("IDGameTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
        
        
def season_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)

        season_id_df = table[["season.id", 
                            "season.year", 
                            "season.type",
                            "season.name",]]
        season_id_df = season_id_df.groupby("season.id").agg(["unique"])
        season_id_df = season_id_df.reset_index()
        season_id_df.columns = season_id_df.columns.droplevel(1)
        season_id_df["season.year"] = season_id_df["season.year"].apply(lambda x: ", ".join(map(str, x)))
        season_id_df["season.type"] = season_id_df["season.type"].apply(lambda x: ", ".join(map(str, x)))
        season_id_df["season.name"] = season_id_df["season.name"].apply(lambda x: ", ".join(map(str, x)))

        season_id_df.to_sql("IDSeasonTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)


def week_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)

        week_id_df = table[["week.id", 
                            "week.sequence", 
                            "week.title", 
                            "season.id", 
                            "season.year", 
                            "season.type"]]
        week_id_df = week_id_df.groupby("week.id").agg(["unique"])
        week_id_df = week_id_df.reset_index()
        week_id_df.columns = week_id_df.columns.droplevel(1)
        week_id_df["week.sequence"] = week_id_df["week.sequence"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df["week.title"] = week_id_df["week.title"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df["season.id"] = week_id_df["season.id"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df["season.year"] = week_id_df["season.year"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df["season.type"] = week_id_df["season.type"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df[["week.sequence", "season.year"]] = week_id_df[["week.sequence", "season.year"]].astype(int)
        week_id_df.sort_values(["season.year", "week.sequence"], inplace=True)
        
        week_id_df.to_sql("IDWeekTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
        
        
def team_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)
    
        team_id_df = table[["team.id", 
                        "team.name", 
                        "team.market", 
                        "team.alias", 
                        "team.sr_id"]]
        team_id_df = team_id_df.groupby("team.id").agg(["unique"])
        team_id_df = team_id_df.reset_index()
        team_id_df.columns = team_id_df.columns.droplevel(1)
        team_id_df["team.name"] = team_id_df["team.name"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["team.market"] = team_id_df["team.market"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["team.alias"] = team_id_df["team.alias"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["team.sr_id"] = team_id_df["team.sr_id"].apply(lambda x: ", ".join(map(str, x)))

        
        team_id_df.to_sql("IDTeamTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)

        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)


def venue_table(engine):
    
    try:
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)
        
        venue_id_df = table[["venue.id", 
                             "venue.name",
                             "venue.city", 
                             "venue.state",
                             "venue.country",
                             "venue.zip",
                             "venue.address",
                             "venue.capacity",
                             "venue.surface",
                             "venue.roof_type",
                             "venue.sr_id", 
                             "venue.location.lat", 
                             "venue.location.lng"]]
        venue_id_df = venue_id_df.groupby("venue.id").agg(["unique"])
        venue_id_df = venue_id_df.reset_index()
        venue_id_df.columns = venue_id_df.columns.droplevel(1)
        venue_id_df["venue.name"] = venue_id_df["venue.name"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.city"] = venue_id_df["venue.city"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.state"] = venue_id_df["venue.state"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.country"] = venue_id_df["venue.country"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.zip"] = venue_id_df["venue.zip"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.address"] = venue_id_df["venue.address"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.capacity"] = venue_id_df["venue.capacity"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.surface"] = venue_id_df["venue.surface"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.roof_type"] = venue_id_df["venue.roof_type"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.sr_id"] = venue_id_df["venue.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.location.lat"] = venue_id_df["venue.location.lat"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.location.lng"] = venue_id_df["venue.location.lng"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["venue.capacity"] = venue_id_df["venue.capacity"].astype(int)
        
        venue_id_df.to_sql("IDVenueTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)

        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
        
        
def calendar_table(start_date, end_date, engine):
    
    try:
        start_time = time.time()

        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        calendar_df = pd.DataFrame(pd.date_range(start_date, end_date, freq="H", normalize=True), columns=["date"])
        calendar_df["day.number"] = calendar_df["date"].dt.day
        calendar_df["day.abr"] = calendar_df["date"].dt.strftime("%a")
        calendar_df["day.name"] = calendar_df["date"].dt.strftime("%A")
        calendar_df["month.number"] = calendar_df["date"].dt.month
        calendar_df["month.abr"] = calendar_df["date"].dt.strftime("%b")
        calendar_df["month.name"] = calendar_df["date"].dt.strftime("%B")
        calendar_df["year"] = calendar_df["date"].dt.year
        calendar_df["week.number"] = calendar_df["date"].dt.isocalendar().week
        calendar_df["day.year.number"] = calendar_df["date"].dt.dayofyear
        calendar_df["hour.day"] = calendar_df["date"].dt.strftime("%H")
        calendar_df = calendar_df.astype(str)

        calendar_df.to_sql("IDCalendarTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)

        print("Calendar Table Done: ", ((time.time()-start_time)/60), sep="\n")
        
    except Exception as e:
        print("Error: ", e, sep="\n")
    
    
def practice_participation(season_start, season_end, engine):
    
    player_participation = practice_par(season_start, season_end)
    player_participation.to_sql("playerPractice", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
    
    
def weekly_stats_offense(conn, engine):
    
    start_time = time.time()
    
    try:
    
        player_snaps = pd.read_sql_table("playerSnaps", con=conn)
        player_practice = pd.read_sql_table("playerPractice", con=conn)
        player_stats = pd.read_sql_table("playerStats", con=conn)

        replace = [" III", " II", " IV", " V", " Jr.", " Sr.", " Sr", " Jr"]
        player_snaps["player_name"] = player_snaps["player_name"].str.replace("|".join([re.escape(s) for s in replace]), "", regex=True)
        player_practice["player.name"] = player_practice["player.name"].str.replace("|".join([re.escape(s) for s in replace]), "", regex=True)
        player_stats["player.name"] = player_stats["player.name"].str.replace("|".join([re.escape(s) for s in replace]), "", regex=True)

        weekly_stats = player_stats.merge(player_snaps, 
                                          how="outer", 
                                          left_on=["player.name", "season.id", "week.id", "team.id", "opp.id"], 
                                          right_on=["player_name", "season.id", "week.id", "game.team.id", "game.opp.id"], 
                                          suffixes=(None, ".merged"))

        weekly_stats["player.name"] = weekly_stats["player.name"].fillna(weekly_stats["player_name"])
        weekly_stats["team.alias"] = weekly_stats["team.alias"].fillna(weekly_stats["game.team.alias"])
        weekly_stats["team.id"] = weekly_stats["team.id"].fillna(weekly_stats["game.team.id"])
        weekly_stats["opp.alias"] = weekly_stats["opp.alias"].fillna(weekly_stats["game.opp.alias"])
        weekly_stats["opp.id"] = weekly_stats["opp.id"].fillna(weekly_stats["game.opp.id"])
        weekly_stats["game.id"] = weekly_stats["game.id"].fillna(weekly_stats["game.id.merged"])
        weekly_stats["season.year"] = weekly_stats["season.year"].fillna(weekly_stats["season.year.merged"])
        weekly_stats["week.sequence"] = weekly_stats["week.sequence"].fillna(weekly_stats["week.sequence.merged"])

        cols = []
        for col in weekly_stats.columns:
            if ".merged" not in col:
                cols.append(col)
        cols.remove("game.team.alias")
        cols.remove("game.team.id")
        cols.remove("game.opp.alias")
        cols.remove("game.opp.id")
        cols.remove("player_name")
        weekly_stats = weekly_stats[cols]

        weekly_stats.rename(columns={"player_url": "player.address"}, inplace=True)
        weekly_stats["season.year"] = weekly_stats["season.year"].astype(str)
        weekly_stats["week.sequence"] = weekly_stats["week.sequence"].astype(str)

        weekly_stats = weekly_stats.merge(
            player_practice, 
            how="outer", 
            left_on=["player.name", "team.alias", "season.year", "week.sequence", "opp.alias"],
            right_on=["player.name", "team", "season", "week", "opp"],
            suffixes=(None, ".merged")
        )

        weekly_stats["team.alias"] = weekly_stats["team.alias"].fillna(weekly_stats["team"])
        weekly_stats["season.year"] = weekly_stats["season.year"].fillna(weekly_stats["season"])
        weekly_stats["week.sequence"] = weekly_stats["week.sequence"].fillna(weekly_stats["week"])
        weekly_stats["opp.alias"] = weekly_stats["opp.alias"].fillna(weekly_stats["opp"])
        weekly_stats["home.away"] = weekly_stats["home.away"].fillna(weekly_stats["home.away.merged"])
        weekly_stats["player.position"] = weekly_stats["player.position"].fillna(weekly_stats["player.pos"])
        weekly_stats = weekly_stats.drop(["team", "season", "week", "opp", "home.away.merged", "player.pos"], axis=1)

        weekly_stats[
            ["off.snaps",
            "def.snaps",
            "st.snaps"]
        ] = weekly_stats [
            ["off.snaps",
            "def.snaps",
            "st.snaps"]
        ].fillna(0)

        weekly_stats[
            ["off.snaps",
            "def.snaps",
            "st.snaps"]
        ] = weekly_stats [
            ["off.snaps",
            "def.snaps",
            "st.snaps"]
        ].replace("", 0)

        weekly_stats[
            ["off.snaps", 
            "def.snaps", 
            "st.snaps"]
        ] = weekly_stats[
            ["off.snaps", 
            "def.snaps", 
            "st.snaps"]
        ].astype(int)

        weekly_stats[
            "player.jersey_num"
        ] = weekly_stats[
            "player.jersey_num"
        ].astype(str)

        columns = list(weekly_stats.select_dtypes(include=[np.number]).columns)
        weekly_stats[columns] = weekly_stats[columns].replace(0, np.nan)
        weekly_stats = weekly_stats.fillna(np.nan)

        weekly_stats[[
            "season.id"
        ]] = weekly_stats.groupby([
            "season.year"
        ])[[
            "season.id"
        ]].apply(lambda x: x.ffill().bfill())


        weekly_stats[[
            "week.id"
        ]] = weekly_stats.groupby([
            "season.id",
            "week.sequence"
        ])[[
            "week.id"
        ]].apply(lambda x: x.ffill().bfill())


        weekly_stats[[
            "team.id",
            "team.name"
        ]] = weekly_stats.groupby([
            "season.id",
            "team.alias"
        ])[[
            "team.id",
            "team.name"
        ]].apply(lambda x: x.ffill().bfill())


        weekly_stats[[
            "game.id",
            "opp.alias",
            "opp.id",
            "opp.name"
        ]] = weekly_stats.groupby([
            "season.id",
            "week.id",
            "team.id"
        ])[[
            "game.id",
            "opp.alias",
            "opp.id",
            "opp.name"
        ]].apply(lambda x: x.ffill().bfill())


        weekly_stats[[
            "player.id", 
            "player.sr_id", 
            "player.jersey_num", 
            "player.address"
        ]] = weekly_stats.groupby([
            "player.name", 
            "season.id",
            "team.id"
        ])[[
            "player.id",                       
            "player.sr_id", 
            "player.jersey_num", 
            "player.address"
        ]].apply(lambda x: x.ffill().bfill())


        weekly_stats[[
            "home.away",
            "game.scheduled",
            "off.snaps",
            "def.snaps",
            "st.snaps",
            "injury",
            "date1",
            "date1.status",
            "date2",
            "date2.status",
            "date3",
            "date3.status",
            "game.status",
            "player.jersey_num"
        ]] = weekly_stats.groupby([
            "player.id",
            "week.id"
        ])[[
            "home.away",
            "game.scheduled",
            "off.snaps",
            "def.snaps",
            "st.snaps",
            "injury",
            "date1",
            "date1.status",
            "date2",
            "date2.status",
            "date3",
            "date3.status",
            "game.status",
            "player.jersey_num"
        ]].apply(lambda x: x.ffill().bfill())
        
        weekly_stats["game.scheduled"] = weekly_stats.groupby(
            "game.id"
        )["game.scheduled"].apply(lambda x: x.ffill().bfill())


        weekly_stats["date1"] = weekly_stats["date1"] + "/" + weekly_stats["season.year"].astype(str)
        weekly_stats["date2"] = weekly_stats["date2"] + "/" + weekly_stats["season.year"].astype(str)
        weekly_stats["date3"] = weekly_stats["date3"] + "/" + weekly_stats["season.year"].astype(str)
        weekly_stats["date1"] = pd.to_datetime(weekly_stats["date1"])
        weekly_stats["date2"] = pd.to_datetime(weekly_stats["date2"])
        weekly_stats["date3"] = pd.to_datetime(weekly_stats["date3"])
        weekly_stats.loc[weekly_stats["date1"].dt.month == 1, "date1"] = weekly_stats["date1"] + pd.offsets.DateOffset(years=1)
        weekly_stats.loc[weekly_stats["date2"].dt.month == 1, "date2"] = weekly_stats["date2"] + pd.offsets.DateOffset(years=1)
        weekly_stats.loc[weekly_stats["date3"].dt.month == 1, "date3"] = weekly_stats["date3"] + pd.offsets.DateOffset(years=1)

        weekly_stats = weekly_stats[
            (weekly_stats["season.year"] == "2016") |
            (weekly_stats["season.year"] == "2017") |
            (weekly_stats["season.year"] == "2018") |
            (weekly_stats["season.year"] == "2019") |
            (weekly_stats["season.year"] == "2020")]

        weekly_stats["player.position"] = weekly_stats["player.position"].replace(
            {
                "OLB": "LB", "CB": "DB", "DE": "DL", "DT": "DL", "S": "DB", "T": "OL",
                "G": "OL", "G/T": "OL", "WR/RS": "WR", "C": "OL", "$LB": "LB", "C/G": "OL",
                "NT": "DL", "ILB": "LB", "FS": "DB", "SS": "DB", "WR/PR": "WR", "DB/LB": "DB",
                "G/C": "OL", "FB/DL": "DL", "DE/LB": "LB", "OT": "OL", "HB": "RB", "T/G": "OL",
                "CB/S": "DB", "DT/DE": "DL", "DE/DT": "DL", "WR/KR": "WR", "LB/DE": "LB",
                "LOLB": "LB", "TE/WR": "TE", "DB/RS": "DB", "H/B": "RB", "S/CB": "DB", 
                "RB/KR": "RB", "LS/TE": "TE", "FB/RB": "RB", "MLB": "LB", "TE/LS": "TE",
                "NT/DE": "DL", "DL/FB": "DL", "G/OT": "OL", "RB/RS": "RB", "LB/S": "DB", 
                "G/DT": "OL", "DL/OL": "DL", "RT": "OL", "RB/WR": "RB", "LB/FB": "LB",
                "CB/RS": "DB", "TE/DE": "DL", "DE/NT": "DL", "WR/RB": "RB", "FB/TE": "TE", 
                "OT/G": "OL", "OG": "OL"
            }
        )

        weekly_stats[
            "player.position"
        ] = weekly_stats.groupby(
            "player.name"
        )[
            "player.position"
        ].apply(lambda x: x.ffill().bfill())

        weekly_stats = weekly_stats[weekly_stats["player.id"].notna()]

        weekly_stats["total.snaps"] = weekly_stats["off.snaps"].add(weekly_stats["st.snaps"], fill_value=0)

        positions = ["RB", "QB", "WR", "TE", "K"]

        weekly_stats = weekly_stats[weekly_stats["player.position"].isin(positions)]

        off_columns = []
        for i in weekly_stats.columns:
            if "defense." not in i:
                if "def." not in i:
                    if "int_r" not in i:
                        off_columns.append(i)

        weekly_stats = weekly_stats[off_columns]

        stat_columns = list(weekly_stats.select_dtypes(include=[np.number]).columns)
        id_columns = list(weekly_stats.select_dtypes(include=["object"]).columns)

        weekly_stats[
            stat_columns[:-1]
        ] = weekly_stats.groupby(
            id_columns[:-8]
        )[
            stat_columns[:-1]
        ].apply(lambda x: x.ffill().bfill())

        weekly_stats[stat_columns] = weekly_stats[stat_columns].replace(0, np.nan)

        weekly_stats["stats.sum"] = weekly_stats[stat_columns[:-3]].sum(axis=1)
        weekly_stats.sort_values(["player.name", "season.year", "week.sequence", "stats.sum"], inplace=True)
        weekly_stats.drop_duplicates(subset=["player.id", "week.id"], keep="last", inplace=True)
        weekly_stats = weekly_stats[(weekly_stats["stats.sum"] != 0) | (weekly_stats["total.snaps"] > 0) | ~(weekly_stats["injury"].isnull())]

        weekly_stats.loc[
        weekly_stats["injury"].str.contains(
                "concussion|head", 
                case=False, 
                na=False,
                regex=True), 
            "head"] = 1
        weekly_stats["head"] = weekly_stats["head"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "face|mouth|jaw|tooth|teeth|togue|eye|nose|cheek|ear",
                case=False, 
                na=False,
                regex=True),
            "face"] = 1
        weekly_stats["face"] = weekly_stats["face"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "neck|throat|cervical|sternoclavi", 
                case=False,  
                na=False,
                regex=True), 
            "neck"] = 1
        weekly_stats["neck"] = weekly_stats["neck"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "shoulder|collarbone|clavicle", 
                case=False,  
                na=False,
                regex=True), 
            "shoulder"] = 1
        weekly_stats["shoulder"] = weekly_stats["shoulder"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "bicep|tricep", 
                case=False,  
                na=False,
                regex=True), 
            "upper_arm"] = 1
        weekly_stats["upper_arm"] = weekly_stats["upper_arm"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "elbow", 
                case=False,  
                na=False,
                regex=True), 
            "elbow"] = 1
        weekly_stats["elbow"] = weekly_stats["elbow"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "forearm|radius|ulna", 
                case=False,  
                na=False,
                regex=True), 
            "forearm"] = 1
        weekly_stats["forearm"] = weekly_stats["forearm"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "wrist|carpal", 
                case=False,  
                na=False,
                regex=True), 
            "wrist"] = 1
        weekly_stats["wrist"] = weekly_stats["wrist"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "hand|finger", 
                case=False,  
                na=False,
                regex=True), 
            "hand_finger"] = 1
        weekly_stats["hand_finger"] = weekly_stats["hand_finger"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "thumb", 
                case=False,  
                na=False,
                regex=True), 
            "thumb"] = 1
        weekly_stats["thumb"] = weekly_stats["thumb"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "back|lumbar", 
                case=False,  
                na=False,
                regex=True), 
            "back"] = 1
        weekly_stats["back"] = weekly_stats["back"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "chest|pec|sternum|rib", 
                case=False,  
                na=False,
                regex=True), 
            "chest"] = 1
        weekly_stats["chest"] = weekly_stats["chest"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "core|appendix|oblique|abdomen", 
                case=False,  
                na=False,
                regex=True), 
            "abdomen"] = 1
        weekly_stats["abdomen"] = weekly_stats["abdomen"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "hip|groin|glute|pelvis", 
                case=False,  
                na=False,
                regex=True), 
            "hip"] = 1
        weekly_stats["hip"] = weekly_stats["hip"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "groin", 
                case=False,  
                na=False,
                regex=True), 
            "groin"] = 1
        weekly_stats["groin"] = weekly_stats["groin"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "quad", 
                case=False,  
                na=False,
                regex=True), 
            "quadricep"] = 1 
        weekly_stats["quadricep"] = weekly_stats["quadricep"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "hamstring", 
                case=False,  
                na=False,
                regex=True), 
            "hamstring"] = 1 
        weekly_stats["hamstring"] = weekly_stats["hamstring"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "thigh|quad|hams", 
                case=False,  
                na=False,
                regex=True), 
            "thigh"] = 1 
        weekly_stats["thigh"] = weekly_stats["thigh"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "knee|acl|mcl|lcl", 
                case=False,  
                na=False,
                regex=True), 
            "knee"] = 1
        weekly_stats["knee"] = weekly_stats["knee"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "calf|shin|tibia|fibula|gastroc|lower leg", 
                case=False,  
                na=False,
                regex=True), 
            "lower_leg"] = 1
        weekly_stats["lower_leg"] = weekly_stats["lower_leg"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "achil", 
                case=False,  
                na=False,
                regex=True), 
            "achilles"] = 1
        weekly_stats["achilles"] = weekly_stats["achilles"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "ankle", 
                case=False,  
                na=False,
                regex=True), 
            "ankle"] = 1
        weekly_stats["ankle"] = weekly_stats["ankle"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "foot|heel", 
                case=False,  
                na=False,
                regex=True), 
            "foot"] = 1
        weekly_stats["foot"] = weekly_stats["foot"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "toe", 
                case=False,  
                na=False,
                regex=True), 
            "toe"] = 1
        weekly_stats["toe"] = weekly_stats["toe"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "not injury|rest|manage|personal|coach", 
                case=False,  
                na=False,
                regex=True), 
            "no_injury"] = 1
        weekly_stats["no_injury"] = weekly_stats["no_injury"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "non football", 
                case=False,  
                na=False,
                regex=True), 
            "non_football"] = 1
        weekly_stats["non_football"] = weekly_stats["non_football"].fillna(0)
        weekly_stats.loc[
            weekly_stats["injury"].str.contains(
                "ill|stomach", 
                case=False,  
                na=False,
                regex=True), 
            "illness"] = 1
        weekly_stats["illness"] = weekly_stats["illness"].fillna(0)
        weekly_stats["total.snaps"] = weekly_stats["off.snaps"].add(weekly_stats["st.snaps"], fill_value=0)
        stat_columns = list(weekly_stats.select_dtypes(include=[np.number]).columns)[:-31]
        weekly_stats["stats.sum"] = weekly_stats[stat_columns].sum(axis=1)

        played_mask = (
            (
                (weekly_stats["total.snaps"].notna()) 
                & (weekly_stats["total.snaps"] > 0)
            ) 
            & (
                (weekly_stats["stats.sum"].notna()) 
                & (weekly_stats["stats.sum"] != 0)
            )
        )

        weekly_stats.loc[played_mask, "played"] = 1
        weekly_stats["played"] = weekly_stats["played"].fillna(0)

        weekly_stats.drop("stats.sum", axis=1, inplace=True)

        weekly_stats["pass.fan_pts"] = (
            (weekly_stats["passing.yards"].fillna(0) * 0.04)
            + (weekly_stats["passing.touchdowns"].fillna(0) * 4)
            + (weekly_stats["passing.interceptions"].fillna(0) * -1)
        )

        weekly_stats["rush.fan_pts"] = (
            (weekly_stats["rushing.yards"].fillna(0) * 0.1)
            + (weekly_stats["rushing.touchdowns"].fillna(0) * 6)
        )

        weekly_stats["rec.non.fan_pts"] = (
             (weekly_stats["receiving.receptions"].fillna(0) * 0)
            + (weekly_stats["receiving.yards"].fillna(0) * 0.1)
            + (weekly_stats["receiving.touchdowns"].fillna(0) * 6)
        )

        weekly_stats["rec.half.fan_pts"] = (
             (weekly_stats["receiving.receptions"].fillna(0) * 0.5)
            + (weekly_stats["receiving.yards"].fillna(0) * 0.1)
            + (weekly_stats["receiving.touchdowns"].fillna(0) * 6)
        )

        weekly_stats["rec.full.fan_pts"] = (
             (weekly_stats["receiving.receptions"].fillna(0) * 1)
            + (weekly_stats["receiving.yards"].fillna(0) * 0.1)
            + (weekly_stats["receiving.touchdowns"].fillna(0) * 6)
        )

        weekly_stats["kick.yrds.fan_pts"] = (
             (weekly_stats["extra_points.kicks.made"].fillna(0) * 1)
            + (weekly_stats["field_goals.yards"].fillna(0) * 0.1)
        )

        weekly_stats["kick.std.fan_pts"] = (
             (weekly_stats["extra_points.kicks.made"].fillna(0) * 1)
            + (weekly_stats["field_goals.made_19"].fillna(0) * 3)
            + (weekly_stats["field_goals.made_29"].fillna(0) * 3)
            + (weekly_stats["field_goals.made_39"].fillna(0) * 3)
            + (weekly_stats["field_goals.made_49"].fillna(0) * 4)
            + (weekly_stats["field_goals.made_50"].fillna(0) * 5)
        )

        weekly_stats["misc.fan_pts"] = (
             (weekly_stats["fumbles.lost_fumbles"].fillna(0) * -2)
            + (weekly_stats["kick_returns.touchdowns"].fillna(0) * 6)
            + (weekly_stats["punt_returns.touchdowns"].fillna(0) * 6)
            + (weekly_stats["extra_points.conversions.successes"].fillna(0) * 2)
        )

        weekly_stats["total_fan_pts.half.kick_std"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.half.fan_pts"]
            + weekly_stats["kick.std.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["total_fan_pts.non.kick_std"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.non.fan_pts"]
            + weekly_stats["kick.std.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["total_fan_pts.full.kick_std"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.full.fan_pts"]
            + weekly_stats["kick.std.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["total_fan_pts.half.kick_yrds"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.half.fan_pts"]
            + weekly_stats["kick.yrds.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["total_fan_pts.non.kick_yrds"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.non.fan_pts"]
            + weekly_stats["kick.yrds.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["total_fan_pts.full.kick_yrds"] = (
            weekly_stats["pass.fan_pts"]
            + weekly_stats["rush.fan_pts"]
            + weekly_stats["rec.full.fan_pts"]
            + weekly_stats["kick.yrds.fan_pts"]
            + weekly_stats["misc.fan_pts"]
        )

        weekly_stats["game.scheduled"] = pd.to_datetime(weekly_stats["game.scheduled"], format="%Y-%m-%d %H:%M")
        weekly_stats["game.scheduled"] = weekly_stats["game.scheduled"].astype(str)

        weekly_stats.to_sql("weeklyStats", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Done: Weekly Stats updated.", "Time: ", ((time.time()-start_time)/60), sep="\n")
        
    except Exception as e:
        print("Error: " + e, "Time to error: ", ((time.time()-start_time)/60), sep="\n")