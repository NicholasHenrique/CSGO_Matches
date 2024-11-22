SELECT PlayerId,
        MatchName,
        COUNT(DISTINCT GameId) AS qtGames,
        MIN(DateTime) AS firstMatch,
        MAX(DateTime) AS lastMatch,
        AVG(FantasyPoints) AS avgPoints, 
        AVG(Kills) AS avgKills, 
        AVG(Assists) AS avgAssists, 
        AVG(Deaths) AS avgDeaths, 
        AVG(Headshots) AS avgHeadshots, 
        AVG(AverageDamagePerRound) AS avgAverageDamagePerRound, 
        AVG(Kast) AS avgKast, 
        AVG(Rating) AS avgRating
FROM bronze.tb_leaderboards
GROUP BY PlayerId, MatchName;