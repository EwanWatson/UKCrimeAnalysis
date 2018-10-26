USE crime; 
GO

-- Create Dimensions

-------------------------------------------------------------------------------
-- outcome dimension
-------------------------------------------------------------------------------

if object_id('outcomeDIM') is not null
drop table dbo.reportedbyDIM

 -- pulling out the reported by field from crime field
select distinct
	[outcome]
into outcomeDIM
from [dbo].[cleanCrimeData]

-- Adding ID
alter table outcomeDIM
add outcomeID int not null identity primary key
go

-------------------------------------------------------------------------------
-- reported by dimension
-------------------------------------------------------------------------------

IF OBJECT_ID('reportedbyDIM') IS NOT NULL
    DROP TABLE dbo.reportedbyDIM;

-- pulling out the reported by field from crime field
SELECT DISTINCT 
       [reportedBy]
INTO reportedbyDIM
FROM [dbo].[cleanCrimeData];

-- Adding ID
ALTER TABLE reportedbyDIM
ADD reportedbyID INT NOT NULL IDENTITY PRIMARY KEY;
GO

--------------------------------------------------------------------------------------
-- falls within dimension
--------------------------------------------------------------------------------------

IF OBJECT_ID('fallswithinDIM') IS NOT NULL
    DROP TABLE dbo.fallswithinDIM;

-- pulling out the falls within field from crime table
SELECT DISTINCT 
       [fallswithin]
INTO fallswithinDIM
FROM [dbo].[cleanCrimeData];

-- adding ID
ALTER TABLE fallswithinDIM
ADD fallswithinID INT NOT NULL IDENTITY PRIMARY KEY;
GO

------------------------------------------------------------------------------------
-- crimeType dimension and enrichment
------------------------------------------------------------------------------------

IF OBJECT_ID('crimetypeDIM') IS NOT NULL
    DROP TABLE dbo.crimetypeDIM;

-- pulling out crimeType field from crime table
SELECT DISTINCT 
       [crimeType],
       CASE -- enriching with a severity rating for each crime
           WHEN [crimeType] = 'Anti-social behaviour'
           THEN 1
           WHEN [crimeType] = 'Burglary'
           THEN 5
           WHEN [crimeType] = 'Criminal damage and arson'
           THEN 6
           WHEN [crimeType] = 'Drugs'
           THEN 3
           WHEN [crimeType] = 'Other crime'
           THEN 4
           WHEN [crimeType] = 'Other theft'
           THEN 4
           WHEN [crimeType] = 'Possession of weapons'
           THEN 5
           WHEN [crimeType] = 'Public disorder and weapons'
           THEN 3
           WHEN [crimeType] = 'Public order'
           THEN 2
           WHEN [crimeType] = 'Robbery'
           THEN 6
           WHEN [crimeType] = 'Shoplifting'
           THEN 2
           WHEN [crimeType] = 'Vehicle crime'
           THEN 5
           WHEN [crimeType] = 'Violence and sexual offences'
           THEN 7
       END [severity],
       CASE -- enriching with a theft vs non-theft flag
           WHEN [crimeType] IN('Bicycle theft', 'Burglary', 'Other theft', 'Robbery', 'Shoplifting')
           THEN 1
           ELSE 0
       END [isStealing],
       CASE
           WHEN [crimeType] IN('Anti-social behaviour', 'Bicycle theft', 'Public disorder and weapons', 'Other theft')
           THEN 1
           ELSE 0
       END [showsCorelation]
INTO crimetypeDIM
FROM [dbo].[cleanCrimeData];

-- adding ID
ALTER TABLE crimetypeDIM
ADD crimetypeID INT NOT NULL IDENTITY PRIMARY KEY;
GO

-----------------------------------------------------------------------------------------
-- weather data dimension
-----------------------------------------------------------------------------------------

IF OBJECT_ID('weatherdataDIM') IS NOT NULL
DROP TABLE dbo.weatherdataDIM;

-- creating a cte to hold the minimum distance between each boundary polygon
-- and a weather station, this will allow me to calculate the weather for each 
-- local authority based on the closest weather station
WITH cte
     AS (SELECT b.ID, 
                b.laCode, 
                b.laName, 
                MIN(b.geoBoundaries.STDistance(w.geoLocation)) [minDistance]
         FROM localAuthorityBoundaries b
              CROSS JOIN weatherData w -- I want each station next to each boundary to calculate minimum distance
         GROUP BY b.ID, 
                  laCode, 
                  laName -- using a group by for each local authority area
)
-- adding in the polygon itself as cannot have geometry data type in a group
-- by
,
     cte2
     AS (SELECT tb.*, 
                b.geoBoundaries
         FROM localAuthorityBoundaries b
              INNER JOIN cte tb ON tb.ID = b.ID)

     -- Creating average weather over each region per month
     SELECT DISTINCT 
            b.ID [boundaryID], 
            d.dateID [dateID], 
            AVG(maxTemp) [maxTemp], 
            AVG(minTemp) [minTemp], 
            (AVG(maxTemp) + AVG(minTemp)) / 2 [averageTemp], -- checked and avg is not weighted towards min/max 
            AVG(airFrostDays) [airFrostDays], 
            AVG(rainfall) [rainfall], 
            AVG(hoursOfSun) [hoursOfSun]
     INTO dbo.weatherdataDIM
     FROM cte2 b
          INNER JOIN [dbo].[weatherData] w -- getting the average weather values for weather stations in each local authority
          -- or taking the closest weather station if none are in the local authority
          ON b.geoBoundaries.STIntersects(w.geoLocation) = 1
          OR b.geoBoundaries.STDistance(w.geoLocation) = b.minDistance
          INNER JOIN [dbo].[dateDIM] d ON d.[month] = w.[month]
                                          AND d.[year] = w.[year]
     GROUP BY ID, 
              [dateID]; -- grouping by ID and date as we want weather for each one of these combinations

-- adding weather ID for Dimension
ALTER TABLE weatherdataDIM
ADD [weatherID] INT NOT NULL IDENTITY PRIMARY KEY;
GO

-------------------------------------------------------------------------------------------
-- Creating and enriching local authority dimension
-------------------------------------------------------------------------------------------

IF OBJECT_ID('boundaryDIM') IS NOT NULL
    DROP TABLE dbo.boundaryDIM;
GO

SELECT b.laCode, 
       b.laName, 
       b.geoBoundaries, 
       p.[Persons] [population], -- enriching with data from other imported table (population data)
       TRY_CAST(d.[Value] AS FLOAT) [deprivationRating] -- data is based on income
INTO [dbo].[boundaryDIM]
FROM [dbo].[LocalAuthorityBoundaries] b
     INNER JOIN [dbo].[population] p ON b.laName = LTRIM(RTRIM(p.[Area name]))
     LEFT JOIN [dbo].[deprivation] d ON LTRIM(RTRIM(d.FeatureCode)) = b.laCode -- Deprivation table has multiple measures so joining on income measure
                                        AND LTRIM(RTRIM(d.[Measurement])) = 'Proportion of Lower-layer Super Output Areas (LSOAs) in most deprived 10% nationally'
                                        AND LTRIM(RTRIM(d.[Indices of deprivation])) = 'b. Income Deprivation Domain'
WHERE b.ID NOT BETWEEN 327 AND 391; -- Do not include Scotland or Wales

ALTER boundaryDIM
ADD [boundaryID] INT NOT NULL IDENTITY PRIMARY KEY;
GO

------------------------------------------------------------------------------
-- Date dimension table made in excel - enrichment
------------------------------------------------------------------------------
IF OBJECT_ID('dateDIM') IS NOT NULL
    DROP TABLE dbo.dateDIM;
GO

SELECT [year], 
       [month],
       CASE -- enrichening with season identifiers
           WHEN [month] IN(12, 1, 2)
           THEN 'Winter'
           WHEN [month] IN(3, 4, 5)
           THEN 'Spring'
           WHEN [month] IN(6, 7, 8)
           THEN 'Summer'
           WHEN [month] IN(9, 10, 11)
           THEN 'Autumn'
       END [Season]
INTO dateDIM
FROM [dbo].[dateTable];

ALTER dateDIM
ADD [dateID] INT NOT NULL IDENTITY PRIMARY KEY;
GO

