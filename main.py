import datetime as dt
import collections

from hh_api.hh_api import *
from db_sql.sql import *

"""
[Shift+F11,F11] Bookmarks(Edit|Bookmarks|Show Line Bookmarks)
"""

def loading(i, len:int):
    with alive_bar(total=len) as bar:
        for i in range(i):
            time.sleep(.005)
            bar()

def load_teams():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")
    data = requests.get(url='https://statsapi.web.nhl.com/api/v1/teams/').json()
    df_data = pd.json_normalize(data['teams'])
    sql_truncate = "truncate table season_22_23.teams; commit;"
    db_sql.sql_execute(sql_truncate)
    sql = ""
    for index, row in df_data.iterrows():

        sql += f"insert into season_22_23.teams(id_team,name,teamname,shortname,firstyearofplay,division_id,division_name,division_nameshort,conference_id,conference_name)" \
              f"values({row['id']},'{row['name']}','{row['teamName']}','{row['shortName']}','{row['firstYearOfPlay']}',{row['division.id']},'{row['division.name']}','{row['division.nameShort']}', " \
              f"{row['conference.id']} , '{row['conference.name']}'); commit; "

        db_sql.sql_execute(sql)
        sql = ""
    print(f"sql: {sql}")

def load_roster_teams():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    sql_truncate = "truncate table season_22_23.team_roster; commit;"
    db_sql.sql_execute(sql_truncate)

    sql_truncate = "truncate table season_22_23.people; commit;"
    db_sql.sql_execute(sql_truncate)

    sql_teams = f"select id_team from season_22_23.teams"
    df_id_teams = db_sql.get_pandas_df(sql=sql_teams)
    length = len(df_id_teams)
    for index, id_team in df_id_teams.iterrows():
        loading(index, length)
        data = requests.get(url=f'https://statsapi.web.nhl.com/api/v1/teams/{id_team[0]}/?expand=team.roster').json()
        df_teams = pd.json_normalize(data['teams'])
        df_roster = pd.json_normalize(df_teams['roster.roster'])

        end_df_roster = len(df_roster.columns)
        sql = ""
        for i in range(0, end_df_roster):
            for row in df_roster[i].values:
                person_fullName = str(row['person.fullName']).replace("'", '')
                try:
                    sql += f"insert into season_22_23.team_roster" \
                           f"(jerseyNumber,person_id,person_fullName,person_link,position_code,position_name,position_type,position_abbreviation)" \
                           f"values({row['jerseyNumber']}, {row['person.id']}, '{person_fullName}', '{row['person.link']}', '{row['position.code']}', '{row['position.name']}', " \
                           f"'{row['position.type']}', '{row['position.abbreviation']}');" \
                           f" commit; "
                except KeyError as e:
                    print(f"error: {e}")
                    print(row['person.fullName'])
        db_sql.sql_execute(sql)
    print(f"insert into season_22_23.team_roster success")

    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    sql_person_links = f"select person_link from season_22_23.team_roster"
    df_person_links = db_sql.get_pandas_df(sql=sql_person_links)
    length = len(df_person_links)
    for i, person_link in df_person_links.iterrows(): #TODO optimize NMP-2, NMP-8
        loading(i, length)
        load_peoples(str(person_link[0]).strip())
    print(f"insert into season_22_23.people success")

def load_peoples(link):
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    url = f"https://statsapi.web.nhl.com" + link
    data = requests.get(url=url).json()
    df_peoples = pd.json_normalize(data['people']) #TODO common insert file from api optimize NMP-2, NMP-8
    lst_cols = []

    for col in df_peoples.columns:
        lst_cols.append(str(col).replace('.', '_'))
    db_sql.insert_rows(schema='season_22_23', table='people', rows=df_peoples.values, target_fields=lst_cols)

def load_standings():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    data = requests.get(url='https://statsapi.web.nhl.com/api/v1/standings').json()
    df_records = pd.json_normalize(data['records'])
    df_teamRecords = pd.json_normalize(df_records['teamRecords'])

    sql_truncate = "truncate table season_22_23.standings; commit;"
    db_sql.sql_execute(sql_truncate)

    sql = ""
    for i in range(0, 8):
        for row in df_teamRecords[i].values:
            sql += f"insert into season_22_23.standings" \
                   f"(team_id,team_name,leaguerecord_wins,leaguerecord_losses,leaguerecord_ot,regulationwins,goalsagainst,goalsscored,points,divisionrank," \
                   f"divisionl10rank,divisionroadrank,divisionhomerank,conferencerank,conferencel10rank,conferenceroadrank,conferencehomerank,leaguerank,leaguel10rank, leagueroadrank," \
                   f"leaguehomerank,wildcardrank,row,gamesplayed,streaktype,streaknumber,streakcode,pointspercentage,ppdivisionrank,ppconferencerank," \
                   f"ppleaguerank,lastupdated)" \
       f"values({row['team.id']},'{row['team.name']}',{row['leagueRecord.wins']},{row['leagueRecord.losses']},{row['leagueRecord.ot']},{row['regulationWins']},{row['goalsAgainst']},{row['goalsScored']}, {row['points']},'{row['divisionRank']}', " \
       f"'{row['divisionL10Rank']}','{row['divisionRoadRank']}','{row['divisionHomeRank']}', '{row['conferenceRank']}','{row['conferenceL10Rank']}','{row['conferenceRoadRank']}','{row['conferenceHomeRank']}','{row['leagueRank']}','{row['leagueL10Rank']}','{row['leagueRoadRank']}'," \
       f"'{row['leagueHomeRank']}', '{row['wildCardRank']}',{row['row']}, {row['gamesPlayed']}, '{row['streak.streakType']}', {row['streak.streakNumber']}, '{row['streak.streakCode']}', {row['pointsPercentage']}, '{row['ppDivisionRank']}'," \
       f"'{row['ppConferenceRank']}','{row['ppLeagueRank']}', '{row['lastUpdated']}');" \
       f" commit; "

    db_sql.sql_execute(sql)
    print(f"insert into season_22_23.standings success")

#R-regular, P-playoff
def load_schedule():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    cur_date = dt.datetime.today()
    delta_date = cur_date + dt.timedelta(days=1)
    url = f'https://statsapi.web.nhl.com/api/v1/schedule?startDate={str(cur_date).split()[0]}&endDate={str(delta_date).split()[0]}'
    data = requests.get(url=url).json()
    df_dates = pd.json_normalize(data['dates'])
    end_dates = len(df_dates)

    sql_truncate = "truncate table season_22_23.schedule; commit;"
    db_sql.sql_execute(sql_truncate)
    sql = ""
    for item_date in range(0, end_dates):
        games = df_dates['games'][item_date].copy()
        end_games = len(games)
        for i in range(0, end_games):

            gamePk = games[i]['gamePk']
            game_status = str(games[i]['status']).replace("'", '"').lower()
            game_teams = str(games[i]['teams']).replace("'", '"').lower()
            game_venue = str(games[i]['venue']).replace("'", '"').lower()
            game_content = str(games[i]['content']).replace("'", '"').lower()
            sql += f"insert into season_22_23.schedule" \
                       f"(gamePk,link,gameType,season,gameDate,status,teams,venue,content)" \
                       f"values({gamePk},\'{games[i]['link']}\',\'{games[i]['gameType']}\',{games[i]['season']},\'{games[i]['gameDate']}\',\'{game_status}\',\'{game_teams}\',\'{game_venue}\',\'{game_content}\');"   \
                       f" commit; "
    db_sql.sql_execute(sql)
    print(f"insert into season_22_23.schedule success")

def load_stats_people_season():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    #seasons = ['20162017','20172018','20182019', '20192020', '20202021', '20212022', '20222023']
    seasons = ['20222023']
    for url_season in seasons:
        sql_people = f"select id, fullname from season_22_23.people where primaryposition_type <> 'Goalie' order by id"
        df_people = db_sql.get_pandas_df(sql=sql_people)
        length = len(df_people)
        for index, row in df_people.iterrows():
            loading(index, length)
            id_people = row[0]
            fullname = row[1]
            #print(f"id_people: {id_people}; fullname: {fullname}")
            url = f"https://statsapi.web.nhl.com/api/v1/people/{id_people}/stats?stats=homeAndAway&season={url_season}"
            #print(url)
            data = requests.get(url=url).json()
            df_stats = pd.json_normalize(data['stats'])
            df_splits = pd.json_normalize(df_stats['splits'][0])

            cols = ["id_person", "fullname_person"]
            lst_cols = list(df_splits.columns).copy()
            for col in lst_cols:
                cols.append(str(col).replace('.', '_'))
            appnd_lst = [int(id_people), str(fullname)]

            del_sql_stats_people_season = f"delete from season_22_23.stats_people_season where id_person = {int(id_people)} and fullname_person = '{str(fullname)}' and season = '{url_season}'; commit;"
            db_sql.sql_execute(del_sql_stats_people_season)

            db_sql.insert_rows(schema='season_22_23', table='stats_people_season', rows=df_splits.values, target_fields=cols, appnd_lst=appnd_lst)

        print(f"insert into season_22_23.stats_people_season season: {url_season} finished")

def load_stats_goalie_season():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")

    #seasons = ['20162017','20172018','20182019', '20192020', '20202021', '20212022', '20222023']
    seasons = ['20222023']
    for url_season in seasons:
        sql_people = f"select id, fullname from season_22_23.people where primaryposition_type = 'Goalie' order by id"
        df_people = db_sql.get_pandas_df(sql=sql_people)
        length = len(df_people)
        for i, row in df_people.iterrows():
            loading(i, length)
            id_people = row[0]
            fullname = row[1]
            url = f"https://statsapi.web.nhl.com/api/v1/people/{id_people}/stats?stats=homeAndAway&season={url_season}"
            #print(url)
            data = requests.get(url=url).json()
            df_stats = pd.json_normalize(data['stats'])
            df_splits = pd.json_normalize(df_stats['splits'][0])
            lst_cols = list(df_splits.columns).copy()
            #generate_create_table_sql(schema='season_22_23', table='stats_goalie_season', columns=lst_cols)

            cols = ["id_person", "fullname_person"]

            for col in lst_cols:
                cols.append(str(col).replace('.', '_'))

            appnd_lst = [int(id_people), str(fullname)]

            del_sql_stats_people_season = f"delete from season_22_23.stats_goalie_season where id_person = {int(id_people)} and fullname_person = '{str(fullname)}' and season = '{url_season}'; commit;"
            db_sql.sql_execute(del_sql_stats_people_season)

            db_sql.insert_rows(schema='season_22_23', table='stats_goalie_season', rows=df_splits.values, target_fields=cols, appnd_lst=appnd_lst)

        loading(length, length)
        print(f"insert into season_22_23.stats_goalie_season season: {url_season} finished")

def load_game_boxscore():
    url = f'https://statsapi.web.nhl.com/api/v1/game/2022020220/feed/live'
    #url = f'https://statsapi.web.nhl.com/api/v1/game/2022020220/boxscore'
    data = requests.get(url=url).json()
    df_gameData = pd.json_normalize(data['gameData'])
    cols_gameData = list(df_gameData.columns).copy()
    cols = [col for col in cols_gameData if str(col).lower().split('.')[0] != 'players']
    #print(cols)

    df_liveData = pd.json_normalize(data['liveData'])
    cols_df_liveData = list(df_liveData.columns).copy()
    cols = [col for col in cols_df_liveData if str(col).lower().split('.')[0] != 'boxscore']

    for row in df_liveData.values:
        for cell in row:
            print(cell)

def main_test():
    Report_row = collections.namedtuple('Report_row', ["id", "fullname"])
    result_list = collections.defaultdict(list)

    sql_people = f"select id, fullname from season_22_23.people order by id"
    df_people = db_exec_get_df(sql=sql_people)

    for ind, row in df_people.iterrows():
        report_row = Report_row(row['id'],row['fullname'])
        result_list[ind].append(report_row)
    rows_lambda = []
    rows_filter = []
    #print(result_list.items())
    for key, rows in result_list.items():
        for r in rows:
            if r.id in (8467950, 8483808):
                rows_lambda.append(list(rows))
            else:
                rows_filter.append(list(rows))
        #f_rows = [row for r in rows if r.id == 8481711]

    print(rows_lambda)
    for r in rows_lambda:
        print(r[0].id)
    #print(rows_filter)
    #print(type(result_list))

    #for r in rows_lambda:
        #print(r[0].id)

    #print(result_list)

def load_regular_stats():
    #TODO add column now() for rows in table
    load_teams()           #one-time load
    load_roster_teams()  #reload TODO  #one-month load

    load_standings()
    load_schedule()

    load_stats_people_season()  #load season '20222023'
    load_stats_goalie_season()  #load season '20222023'

def f_testing():
    db_sql = DbSql("localhost", "nhl_superuser", "nhl_superuser", "nhl")
    main_test() #testing
    load_game_boxscore() #testing
    db_sql.generate_create_table_sql(schema='season_22_23', table='temp', columns=['a', 'b', 'c'])
    load_peoples('/api/v1/people/8475287')
    db_sql.insert_rows(schema='season_22_23', table='temp', rows = ["432",544,"2022-10-11"], target_fields=['a', 'b', 'c'])

if __name__ == "__main__":
    #add logging TODO
    #create class load_regular_stats TODO (param season, type games: R, P)
    """Load stats nhl"""
    #load_regular_stats()

    """Load data from HeadHunter"""
    #get_oauth()
    #main_hh_api()
    #TODO time