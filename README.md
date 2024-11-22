# CSGO Matches

Data Engineering project about CSGO matches.

This project consists of using PySpark to collect official CSGO matches via [API][1]. These matches are stored in their original format in the [_raw_](#raw) layer and, later, manipulated and treated to be stored in the  [_bronze_](#bronze), [_silver_](#silver) and _gold_ layers.

**Code execution order:** [get_match_history.py][3] -> [match_stream.py][12] -> [get_match_details.py][6] -> [process_match_details.py][11] -> [match_details_stream.py][16] -> [silver_ingestion.py][19]

## Raw

Via [API][1], match data in json format is collected and stored in the [_raw_][2] folder. The [get_match_history.py][3] code has the function of collecting matches for the first time, as well as collecting matches from the last 7 days (based on the day of the oldest match stored in [_raw_][4]) and collecting all matches since the most recently stored match to the most current match based on the day the [get_game_history.py][5] code was run.

The [get_match_details.py][6] code collects all match ids to get corresponding match details. It uses [raw/matches_landing][7] to store all collected match details and [raw/matches_proceeded][8] to store each gameID with its correponding location in [raw/matches_landing][7]. This has the objective to avoid acessing the API more two times to create [raw/tb_leaderboards][9] and [raw/tb_maps][10]. To accomplish this, it compares which matches are stored in [bronze/csgo_match_history][13] with the ones that have already been stored in [raw/matches_proceeded][8].

The [process_match_details.py][11] code collects matches id from [raw/matches_proceeded][8] and creates json files in [raw/tb_leaderboards][9] and [raw/tb_maps][10] to store corresponding maps and leaderboards information from each gameID. To accomplish this, it compares which matches are stored in [raw/matches_proceeded][8] with the ones that have already been stored in [raw/tb_leaderboards][9] and [raw/tb_maps][10].

## Bronze

To garantee that in a table only exists distinct matches and the last version of them, the code [match_stream.py][12] utilizes a Spark Window function to filter duplicated matches from [raw/csgo_match_history][2] to [bronze/csgo_match_history][13]. Also, [match_stream.py][12] is responsible for keeping [raw/csgo_match_history][2] and [bronze/csgo_match_history][13] updated with the same matches through a Spark Stream. Similar happens with [match_details_stream.py][16], differs in that in this case it maintains [raw/tb_leaderboards][9] and [raw/tb_maps][10] in sync with [bronze/tb_leaderboards][17] and [bronze/tb_maps][18]. The codes [match_stream.py][12] and [match_details_stream.py][16] are also responsible for determinig the schemas of [bronze/tb_leaderboards][17] and [bronze/tb_maps][18].

All tables in [bronze][14] are stored in [Delta][15] format.

## Silver

In silver layer, two SQL codes are responsible for populating the layer, [sql/fs_players.sql][20] calculates each player's statistics and [sql/fs_teams.sql][21] counts how many matches each team had on each map. [silver_ingestion.py][19] code creates [silver][22] database and performs upsert operations (insert and update) on [silver/fs_players][13] and [silver/fs_teams][13] tables.

All tables in [silver][22] are stored in [Delta][15] format.

[1]: https://sportsdata.io/developers/api-documentation/csgo
[2]: #/raw/csgo_match_history/
[3]: #/get_match_history.py
[4]: #/raw/csgo_match_history/
[5]: #/get_game_details.py
[6]: #/get_match_details.py
[7]: #/raw/matches_landing/
[8]: #/raw/matches_proceeded/
[9]: #/raw/tb_leaderboards/
[10]: #/raw/tb_maps/
[11]: #/process_match_details.py
[12]: #/match_stream.py
[13]: #/spark-warehouse/bronze.db/csgo_match_history/
[14]: #/spark-warehouse/bronze.db/
[15]: https://delta.io/
[16]: #/match_details_stream.py
[17]: #/spark-warehouse/bronze.db/tb_leaderboards/
[18]: #/spark-warehouse/bronze.db/tb_maps/
[19]: #/silver_ingestion.py
[20]: #/sql/fs_players.sql
[21]: #/sql/fs_teams.sql
[22]: #/spark-warehouse/silver.db/
[13]: #/spark-warehouse/silver.db/fs_players/
[13]: #/spark-warehouse/silver.db/fs_teams/