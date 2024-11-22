SELECT TeamName, MapName, COUNT(*) AS matchCount
FROM (
    SELECT TeamAName AS TeamName, Name AS MapName FROM bronze.tb_maps
    UNION ALL
    SELECT TeamBName AS TeamName, Name AS MapName FROM bronze.tb_maps
) AS teams
GROUP BY TeamName, MapName