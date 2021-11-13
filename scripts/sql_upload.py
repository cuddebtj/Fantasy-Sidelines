import pandas as pd
from scrape import *
from clean import get_season


def schedule_stats_api_sql(api_key, year, engine):

    schedule_final = pd.DataFrame()
    players_stats_final = pd.DataFrame()
    teams_stats_final = pd.DataFrame()
    try:
        for yr in year:
            schedule, players_stats, teams_stats = get_season(api_key, yr)

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
        print("Done: Player info scraped.")
        
        try:
            snaps_df = snap_scrape(y1, y2, players_df)
            print("Done: Player snaps scraped.")
            snaps_df.to_sql("playerSnaps", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
            
        except Exception as e_:
            print("Error: Player snaps not scraped", "Error: "+e_, sep="\n")
        
    except Exception as e:
        print("Error: Player info not scraped", "Error: "+e, sep="\n")


def injuries(y1, y2, engine):
    
    injuries_df = pd.DataFrame()
    
    for season in range(y1, y2+1):
        
        injury = injury_scrape(season)
        injuries_df = pd.concat([injuries_df, injury])
    
    injuries_df.to_sql("playerInjuires", engine, if_exists="append", index=False, schema="dbo", chunksize=500)
        
    return injuries_df


def player_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.playerStats", engine)

        player_table_df = table[["player.id", "player.name", "player.position", "player.sr_id", "player.jersey_num"]]
        player_table_df = player_table_df.groupby(["player.id", "player.name", "player.sr_id"])[["player.position", "player.jersey_num"]].agg(["unique"])
        player_table_df = player_table_df.reset_index()
        player_table_df.columns = player_table_df.columns.droplevel(1)
        player_table_df["player.position"] = player_table_df["player.position"].apply(lambda x: ", ".join(map(str, x)))
        player_table_df["player.jersey_num"] = player_table_df["player.jersey_num"].apply(lambda x: ", ".join(map(str, x)))

        player_table_df.to_sql("playerIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
       
    
def game_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)

        game_id_df = table[["game.id", 
                            "game.status", 
                            "game.reference",
                            "game.number",
                            "game.scheduled",
                            "game.attendance",
                            "game.utc_offset",
                            "game.entry_mode",
                            "game.sr_id",
                            "game.broadcast.internet", 
                            "game.broadcast.network"]]
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
        
        game_id_df.to_sql("gameIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
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

        season_id_df.to_sql("seasonIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)


def week_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)

        week_id_df = table[["week.id", 
                            "week.sequence", 
                            "week.title"]]
        week_id_df = week_id_df.groupby("week.id").agg(["unique"])
        week_id_df = week_id_df.reset_index()
        week_id_df.columns = week_id_df.columns.droplevel(1)
        week_id_df["week.sequence"] = week_id_df["week.sequence"].apply(lambda x: ", ".join(map(str, x)))
        week_id_df["week.title"] = week_id_df["week.title"].apply(lambda x: ", ".join(map(str, x)))
        
        week_id_df.to_sql("weekIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)
        
        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)
        
        
def team_table(engine):
    
    try:    
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)
    
        team_id_df = table[["game.summary.team.id", 
                        "game.summary.team.name", 
                        "game.summary.team.market", 
                        "game.summary.team.alias", 
                        "game.summary.team.sr_id"]]
        team_id_df = team_id_df.groupby("game.summary.team.id").agg(["unique"])
        team_id_df = team_id_df.reset_index()
        team_id_df.columns = team_id_df.columns.droplevel(1)
        team_id_df["game.summary.team.name"] = team_id_df["game.summary.team.name"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["game.summary.team.market"] = team_id_df["game.summary.team.market"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["game.summary.team.alias"] = team_id_df["game.summary.team.alias"].apply(lambda x: ", ".join(map(str, x)))
        team_id_df["game.summary.team.sr_id"] = team_id_df["game.summary.team.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        
        team_id_df.to_sql("teamIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)

        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)


def venue_table(engine):
    
    try:
        table = pd.read_sql_query("SELECT * FROM FantasySidelines.dbo.schedule", engine)
        
        venue_id_df = table[["game.venue.id", 
                             "game.venue.name",
                             "game.venue.city", 
                             "game.venue.state",
                             "game.venue.country",
                             "game.venue.zip",
                             "game.venue.address",
                             "game.venue.capacity",
                             "game.venue.surface",
                             "game.venue.roof_type",
                             "game.venue.sr_id", 
                             "game.venue.location.lat", 
                             "game.venue.location.lng"]]
        venue_id_df = venue_id_df.groupby("game.venue.id").agg(["unique"])
        venue_id_df = venue_id_df.reset_index()
        venue_id_df.columns = venue_id_df.columns.droplevel(1)
        venue_id_df["game.venue.name"] = venue_id_df["game.venue.name"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.city"] = venue_id_df["game.venue.city"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.state"] = venue_id_df["game.venue.state"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.country"] = venue_id_df["game.venue.country"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.zip"] = venue_id_df["game.venue.zip"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.address"] = venue_id_df["game.venue.address"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.capacity"] = venue_id_df["game.venue.capacity"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.surface"] = venue_id_df["game.venue.surface"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.roof_type"] = venue_id_df["game.venue.roof_type"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.sr_id"] = venue_id_df["game.venue.sr_id"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.location.lat"] = venue_id_df["game.venue.location.lat"].apply(lambda x: ", ".join(map(str, x)))
        venue_id_df["game.venue.location.lng"] = venue_id_df["game.venue.location.lng"].apply(lambda x: ", ".join(map(str, x)))
        
        venue_id_df.to_sql("venueIDTable", engine, if_exists="replace", index=False, schema="dbo", chunksize=500)

        print("Upload to SQL successful.")
    
    except Exception as e:
        print("Error:")
        print(e)