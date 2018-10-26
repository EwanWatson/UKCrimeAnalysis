USE crime; 
GO

-- These are my views and tables for exporting to tableau
-------------------------------------------------------------------------------
-- Exploratory data
-------------------------------------------------------------------------------

IF OBJECT_ID('vw_Exploration_Data') IS NOT NULL
    DROP VIEW vw_Exploration_Data;
GO

-- create view with average weather stats and crime count for each month and year
-- combination for each crime type for exploratory analysis
CREATE VIEW vw_Exploration_Data
AS
     SELECT AVG(w.averageTemp) [averageTemp], 
            AVG(w.airFrostDays) [airFrostDays], 
            AVG(w.hoursOfSun) [hoursOfSun], 
            AVG(w.rainfall) [rainfall], 
            COUNT(*) [crimeCount], 
            d.[month], 
            d.[year], 
            c.crimeType
     FROM factTable f
          INNER JOIN crimetypeDIM c ON c.crimetypeID = f.crimetypeID
          INNER JOIN weatherdataDIM w ON f.weatherID = w.weatherID
          INNER JOIN dateDIM d ON f.dateID = d.dateID
     GROUP BY d.dateID, 
              c.crimeType, 
              d.[month], 
              d.[year]; 
GO

-------------------------------------------------------------------------------------------------------
-- Map Tables
-------------------------------------------------------------------------------------------------------

IF OBJECT_ID('final.Gradient_Data') IS NOT NULL
    DROP TABLE [final].[Gradient_Data];
GO

WITH cte -- Count of no. of crimes per year for each crime type
     AS (SELECT COUNT(*) OVER(PARTITION BY d.[year], 
                                           b.ID, 
                                           c.crimeType) [NoOfCrimesPerYearPerCrimeType], 
                d.[month], 
                d.[year], 
                b.[boundaryID], 
                w.airFrostDays, 
                w.averageTemp, 
                w.hoursOfSun, 
                w.rainfall, 
                c.crimeType
         FROM factTable f
              INNER JOIN dateDIM d ON d.dateID = f.dateID
              INNER JOIN boundaryDIM b ON b.ID = f.regionID
              INNER JOIN weatherdataDIM w ON w.weatherID = f.weatherID
              INNER JOIN crimetypeDIM c ON f.crimetypeID = c.crimetypeID
         WHERE d.[year] > 2010
               AND c.crimetype NOT IN('Other crime') -- as other crime is not specific and therefore has
),													 -- little value
     cte2 -- Count of number of crimes per month for each crime type
     AS (SELECT CAST(COUNT(*) AS FLOAT) [noOfCrimesPerMonth], 
                CAST(noOfCrimesPerYearPerCrimeType AS FLOAT) [noOfCrimesPerYear], 
                [month], 
                [year], 
                [boundaryID], 
                crimeType, 
                CAST(AVG(averageTemp) AS FLOAT) [averageTemp],-- average measur per month for each area:  
                CAST(AVG(airFrostDays) AS FLOAT) [airFrostDays], -- all measures are the same for each
                CAST(AVG(hoursOfSun) AS FLOAT) [hoursOfSun], -- area and month so this is an easy way to
                CAST(AVG(rainfall) AS FLOAT) [rainfall] -- include the measures with the aggregation 
         FROM cte c										-- (group by) without changing the measure
         GROUP BY [year], 
                  [month], 
                  [boundaryID], 
                  crimeType, 
                  noOfCrimesPerYearPerCrimeType), -- This has no effect on grouping as it is the same for each of the above 
     cte3
     AS (SELECT crimeType,  -- percentage of crimes in the year that occur in a specific month 
                [noOfCrimesPerMonth] * [noOfCrimesPerYear] / (AVG([noOfCrimesPerYear]) OVER(PARTITION BY boundaryID, 
                                                                                                         crimeType)) [normalizedCrimePerMonth],
                AVG([noOfCrimesPerMonth]) OVER(PARTITION BY boundaryID, 
                                                            crimeType) [averageCrimePerMonth], 
                [averageTemp], 
                [airFrostDays], 
                [hoursOfSun], 
                [rainfall], 
                [boundaryID]
         FROM cte2 c),
     cte4 -- This is the gradient calculation using linear regression - again, averages have no effect as each weather measurement in a group by 'batch' is the same
	 -- so are included to allow the group by statement to be made in the same cte
     AS (SELECT(AVG([averageTemp]) * AVG([normalizedCrimePerMonth]) - AVG([averageTemp] * [normalizedCrimePerMonth])) / ((SQUARE(AVG([averageTemp]))) - AVG(SQUARE([averageTemp]))) [gradientOfIncreasedCrimePerMonthTemp],
               CASE -- The case is here to prevent divide by zero errors
                   WHEN AVG([airFrostDays]) = 0
                   THEN NULL
                   ELSE(AVG([airFrostDays]) * AVG([normalizedCrimePerMonth]) - AVG([airFrostDays] * [normalizedCrimePerMonth])) / ((SQUARE(AVG([airFrostDays]))) - AVG(SQUARE([airFrostDays])))
               END [gradientOfIncreasedCrimePerMonthFrost],
               CASE
                   WHEN AVG([hoursOfSun]) = 0
                   THEN NULL
                   ELSE(AVG([hoursOfSun]) * AVG([normalizedCrimePerMonth]) - AVG([hoursOfSun] * [normalizedCrimePerMonth])) / ((SQUARE(AVG([hoursOfSun]))) - AVG(SQUARE([hoursOfSun])))
               END [gradientOfIncreasedCrimePerMonthSun],
               CASE
                   WHEN AVG([rainfall]) = 0
                   THEN NULL
                   ELSE(AVG([rainfall]) * AVG([normalizedCrimePerMonth]) - AVG([rainfall] * [normalizedCrimePerMonth])) / ((SQUARE(AVG([rainfall]))) - AVG(SQUARE([rainfall])))
               END [gradientOfIncreasedCrimePerMonthRain], 
               [boundaryID],  
               crimeType, 
               [averageCrimePerMonth]
         FROM cte3
         GROUP BY boundaryID, 
                  crimeType, 
                  averageCrimePerMonth)
     SELECT [gradientOfIncreasedCrimePerMonthTemp], 
            [gradientOfIncreasedCrimePerMonthFrost], 
            [gradientOfIncreasedCrimePerMonthSun], 
            [gradientOfIncreasedCrimePerMonthRain], 
            crimeType, 
            b.laName, 
            b.geoBoundaries, 
            b.[population], 
            b.[deprivationRating], 
            [averageCrimePerMonth]
     INTO [final].[Gradient_Data]
     FROM cte4 c
          INNER JOIN boundaryDIM b ON c.boundaryID = b.boundaryID;
GO

--------------------------------------------------------------------------------------------------
-- Population density table
--------------------------------------------------------------------------------------------------

IF OBJECT_ID('final.popdentsity') IS NOT NULL
    DROP TABLE [final].[popdensity];
GO

SELECT *, 
       [population] / (geoboundaries.STArea() / 1000000) [PopDensity]
INTO [final].[popdensity]
FROM [final].[Gradient_Data];

-------------------------------------------------------------------------------------------------
-- Actual graph data
-------------------------------------------------------------------------------------------------

-- This table contains similar to the map data. However, the graphs for each Local Authority will
-- show trend line with gradient calculated in tableau an so will not need the gradient calculation 
-- section of the above table, and will need to be less aggregated

IF OBJECT_ID('final.Gradient_Graphs') IS NOT NULL
    DROP TABLE [final].[Gradient_Graphs];
GO

WITH cte
     AS (SELECT COUNT(*) OVER(PARTITION BY d.[year], 
                                           b.boundaryID, 
                                           c.crimeType) [NoOfCrimesPerYearPerCrimeType], 
                d.[month], 
                d.[year], 
                d.[Season], 
                b.boundaryID, 
                w.airFrostDays, 
                w.averageTemp, 
                w.hoursOfSun, 
                w.rainfall, 
                c.crimeType
         FROM factTable f
              INNER JOIN dateDIM d ON d.dateID = f.dateID
              INNER JOIN boundaryDIM b ON b.boundaryID = f.regionID
              INNER JOIN weatherdataDIM w ON w.weatherID = f.weatherID
              INNER JOIN crimetypeDIM c ON f.crimetypeID = c.crimetypeID
         WHERE d.[year] > 2010 
               AND c.crimetype NOT IN('Other crime')),
     cte2
     AS (SELECT CAST(COUNT(*) AS FLOAT) [noOfCrimesPerMonth], 
                CAST(noOfCrimesPerYearPerCrimeType AS FLOAT) [noOfCrimesPerYear], 
                [month], 
                [year], 
                [Season], 
                [boundaryID], 
                crimeType, 
                CAST(AVG(averageTemp) AS FLOAT) [averageTemp],-- average temp per month for each area
                CAST(AVG(airFrostDays) AS FLOAT) [airFrostDays], --  just put the avg to include with group by 
                CAST(AVG(hoursOfSun) AS FLOAT) [hoursOfSun], -- as in the above ma table
                CAST(AVG(rainfall) AS FLOAT) [rainfall]
         FROM cte c
         GROUP BY [year], 
                  [month], 
                  [season], 
                  boundaryID, 
                  crimeType, 
                  noOfCrimesPerYearPerCrimeType)
     SELECT crimeType, -- crimes in the year that occur in the specific month, this allows comparison across years 
            [noOfCrimesPerMonth] * [noOfCrimesPerYear] / (AVG([noOfCrimesPerYear]) OVER(PARTITION BY boundaryID, 
                                                                                                     crimeType)) [normalizedCrimePerMonth],
            [averageTemp], 
            [airFrostDays], 
            [hoursOfSun], 
            [rainfall], 
            b.laName, 
            b.geoBoundaries, 
            b.[population], 
            b.[deprivationRating], 
            [year], 
            [month], 
            [Season]
     INTO [final].[Gradient_Graphs]
     FROM cte2 c
          INNER JOIN boundaryDIM b ON b.boundaryID = c.boundaryID;
GO

---------------------------------------------------------------------------------------------
-- floods table
---------------------------------------------------------------------------------------------
SELECT *
FROM [dbo].[floodTable];
IF OBJECT_ID('final.floods') IS NOT NULL
    DROP TABLE [final].[floods];
GO
SELECT f.latitude, 
       f.longitude, 
       c.crimeType
INTO [final].[floods]
FROM factTable f
     INNER JOIN crimetypeDIM c ON f.crimetypeID = c.crimetypeID
     INNER JOIN dateDIM d ON d.[dateID] = f.dateID
WHERE d.[month] = 2
      AND d.[year] = 2014;

---------------------------------------------------------------------------------------------
--
---------------------------------------------------------------------------------------------