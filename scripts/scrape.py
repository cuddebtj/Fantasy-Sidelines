import requests
import time
import string
import html5lib
import re
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


def player_scrape():
    
    start = time.time()
    
    abc = list(string.ascii_uppercase)
    players_df = pd.DataFrame()
    
    try:
        for z in abc: 

            url = f"https://www.pro-football-reference.com/players/{z}/"
            r = requests.get(url)
            r = r.content
            soup = BeautifulSoup(r, "lxml")
            data = soup.find_all("p")
            players = []

            for i in range(len(data[:-12])):

                try:
                    player = {}
                    player["player_url"] = data[i].find("a")["href"][9:-4]
                    player["player_name"] = data[i].find("a").text
                    player["player_lastyear"] = data[i].text[-4:]
                    player["player_position"] = data[i].text[data[i].text.find("(")+1: data[i].text.find(")")]
                    players.append(player)
                except:
                    continue

            df = pd.DataFrame.from_dict(players)
            players_df = pd.concat([players_df, df])

            time.sleep(1)

        players_df["player_lastyear"] = players_df["player_lastyear"].astype("int64")

        print("Done: players_df")
        print("Time: ", (time.time()-start)/60, " min")

        return players_df

    except Exception as e:
        print("Error: "+e)


def snap_scrape(y1, y2, players_df):
    
    start = time.time()
    
    weeks = pd.read_csv("../nfl-weeks.csv")
    try:
        if players_df.empty == True:
            print("Error: Please insert players database from the return of fucntion player_scrape()")

        else:
            snaps_df = pd.DataFrame()
            players = players_df[players_df["player_lastyear"] >= y1]
            players_url = players["player_url"].tolist()

            for url in players_url:

                for year in range(y1, y2+1):

                    p_url = f"https://www.pro-football-reference.com/players/{url}/gamelog/{year}/"

                    try:
                        r = requests.get(p_url)
                        r = r.content
                        soup = BeautifulSoup(r, "lxml")
                        table = soup.find("table", attrs={"id": "stats"})
                        headers = table.find_all("th", 
                                                 attrs={"data-stat": 
                                                        ["game_date", 
                                                         "team", 
                                                         "game_location", 
                                                         "opp", 
                                                         "offense", 
                                                         "defense", 
                                                         "special_teams"]})
                        columns = []
                        for h in headers:
                            columns.append(h.text)

                        data = table.find_all("tr")[2:-1]
                        rows = []
                        for row in data:
                            r = []
                            td = row.find_all("td", 
                                              attrs={"data-stat": 
                                                     ["game_date", 
                                                      "team", 
                                                      "game_location", 
                                                      "opp", 
                                                      "offense", 
                                                      "defense", 
                                                      "special_teams"]})
                            for d in td:
                                r.append(d.text)
                            rows.append(r)

                        df = pd.DataFrame(rows, columns=columns)
                        df.columns = ["date", "team", "home.away", "opp", "off.snaps", "def.snaps", "st.snaps"]
                        df["player_url"] = url
                        df["season"] = year
                        snaps_df = pd.concat([df, snaps_df])

                    except:
                        continue

                time.sleep(1)

            for row in weeks.iterrows():
                mask = (snaps_df["date"] >= row[1]["Start_Date"]) & (snaps_df["date"] <= row[1]["End_Date"])
                snaps_df.loc[mask, "week"] = row[1]["Week"]

            snaps_df = snaps_df.merge(players, how="left", left_on="player_url", right_on="player_url")
            snaps_df.drop("player_lastyear", axis=1, inplace=True)
                
            snaps_df["season"] = snaps_df["season"].astype(str)
            snaps_df["week"] = snaps_df["week"].astype(str)
            
            snaps_df = snaps_df.replace({"team": {
                "GNB": "GB", "JAX": "JAC", "KAN": "KC", 
                "LAR": "LA", "LVR": "LV", "NOR": "NO", 
                "NWE": "NE", "SDG": "SD", "SFO": "SF", 
                "TAM": "TB"
            }, "opp": {
                "GNB": "GB", "JAX": "JAC", "KAN": "KC", 
                "LAR": "LA", "LVR": "LV", "NOR": "NO", 
                "NWE": "NE", "SDG": "SD", "SFO": "SF", 
                "TAM": "TB"
            }})
            snaps_df["home.away"] = snaps_df["home.away"].replace({"@": "away", "": "home", np.nan: "home"})
            snaps_df[
                ["off.snaps",
                "def.snaps",
                "st.snaps"]
            ] = snaps_df [
                ["off.snaps",
                "def.snaps",
                "st.snaps"]
            ].fillna(0)
            snaps_df[
                ["off.snaps",
                "def.snaps",
                "st.snaps"]
            ] = snaps_df [
                ["off.snaps",
                "def.snaps",
                "st.snaps"]
            ].replace("", 0)
            snaps_df[
                ["off.snaps", 
                "def.snaps", 
                "st.snaps"]
            ] = snaps_df[
                ["off.snaps", 
                "def.snaps", 
                "st.snaps"]
            ].astype(int)
            snaps_df["season"] = snaps_df["season"].astype(str)
            
            replace = [" III", " II", " IV", " V", " Jr.", " Sr.", " Sr", " Jr"]
            snaps_df["player_name"] = snaps_df["player_name"].str.replace("|".join([re.escape(s) for s in replace]), "", regex=True)
            snaps_df = snaps_df.drop_duplicates()

            print("Done: Player snaps scraped.")
            print("Done: ", (time.time()-start)/60, " min")

            return snaps_df
        
    except Exception as e:
        print("Error: "+e)


def injury_scrape(season):
    
    start = time.time()
    
    teams = ["crd", "atl", "rav", "buf", "car", "chi", "cin", "cle",
             "dal", "den", "det", "gnb", "htx", "clt", "jax", "kan", 
             "rai", "sdg", "ram", "mia", "min", "nwe", "nor", "nyg",
             "nyj", "phi", "pit", "sfo", "sea", "tam", "oti", "was"]
    
    injury_df = pd.DataFrame()
    
    for team in teams:
        
        url = f"https://www.pro-football-reference.com/teams/{team}/{season}_injuries.htm"
        r = requests.get(url)
        soup = BeautifulSoup(r.content, "lxml")
        table = soup.find("table", attrs={"id": "team_injuries"})

        table_headers = table.find("thead").find_all("th")
        columns = []
        for header in range(len(table_headers)):
            c1 = table_headers[header].text
            c2 = table_headers[header]["data-stat"]
            columns.append(c1 + ": " +c2)
        columns.insert(0, "player_address")

        table_rows = table.find("tbody").find_all("tr")
        rows = []
        for tr in range(len(table_rows)):
            p = table_rows[tr].find("th").text
            padd =table_rows[tr].find("th").find("a")["href"][9:-4]
            td = table_rows[tr].find_all("td")
            row = [td[d].get("data-tip") if td[d].has_attr("data-tip") else np.nan for d in range(len(td))]
            row.insert(0, p)
            row.insert(0, padd)
            rows.append(row)

        df = pd.DataFrame(rows, columns=columns)
        
        final_df = pd.melt(df, id_vars=columns[0:2], value_vars=columns[2:], value_name="injury")
        final_df = final_df.dropna(subset=["injury"]).reset_index(drop=True)
        final_df.columns = ["player_address", "player.name", "variable", "injury"]
        
        final_df["season"] = season
        final_df["team"] = team
        
        final_df[["date.opp", "week"]] = final_df["variable"].str.split(": week_", 1, expand=True)
        final_df[["status", "injury"]] = final_df["injury"].str.split(": ", 1, expand=True)
        final_df[["date", "opp"]] = final_df["date.opp"].str.split("vs. ", 1, expand=True)
        final_df["date"] = final_df["date"].astype(str) + "/" + final_df["season"].astype(str)
        
        final_df.drop(["variable", "date.opp"], axis=1, inplace=True)
        final_df = final_df[["player_address", "player.name", "date", "season", "week", "team", "opp", "status", "injury"]]
        
        injury_df = pd.concat([injury_df, final_df], ignore_index=True)
        
    replace = [" III", " II", " Jr.", " Sr.", " Sr", " Jr"]
    injury_df["player.name"] = injury_df["player.name"].str.replace("|".join([re.escape(s) for s in replace]), "", regex=True)
    injury_df = injury_df.drop_duplicates()
            
    print(f"Done with season: {season}\n", f"Time: {(time.time()-start)/60} minutes.")
        
    return injury_df