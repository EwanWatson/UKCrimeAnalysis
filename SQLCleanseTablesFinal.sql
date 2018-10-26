USE crime;
GO
--------------------------------------------------------------------------
--CREATE SCHEMA trash;
--GO
--CREATE SCHEMA final;
--GO
-- no longer needed for after first run through
--------------------------------------------------------------------------

-- Drop foreign key constraints so that tables
-- can be dropped and remade
if object_id('fk_boundaryID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_boundaryID

if object_id('fk_crimeTypeID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_crimeTypeID

if object_id('fk_dateID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_dateID

if object_id('fk_fallswithinID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_fallswithinID

if object_id('fk_reportedbyID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_reportedbyID

if object_id('fk_weathedataID') is not null
ALTER TABLE factTable
DROP CONSTRAINT fk_weatherdataID

if object_id('fk_outcomeID') is not null
ALTER TABLE outcomeDIM
DROP CONSTRAINT fk_outcomeID

if object_id('fk_floodID') is not null
ALTER TABLE floodTable
DROP CONSTRAINT fk_floodID

--------------------------------------------------------------------------
-- Cleaning the crimeData table
--------------------------------------------------------------------------
-- Columns needed for further analysis are selected,
-- and latitude and longitude are converted to geography
-- data type in order to locate the closest weather station to
-- each crime to get the average weather for that month 
-- for that area
-- crime data has a text qualifier of "
-- add in a new crime ID col. as most ID's are empty,
-- and the crime ID's linking to outcome are not useful
-- here anyway

IF OBJECT_ID('cleanCrimeData') IS NOT NULL
    DROP TABLE cleanCrimeData;

SELECT TRY_CAST(SUBSTRING(LTRIM(RTRIM([Month])), 1, 4) AS INT) [year], -- Splitting year and month 
       TRY_CAST(SUBSTRING(LTRIM(RTRIM([Month])), CHARINDEX('-', LTRIM(RTRIM([Month])))+1, LEN(LTRIM(RTRIM([Month])))-CHARINDEX('-', LTRIM(RTRIM([Month])))) AS INT) [month], 
       LTRIM(RTRIM(ISNULL([Reported by], [Falls within]))) [reportedBy], -- trimming string columns 
       LTRIM(RTRIM(ISNULL([Falls within], [Reported by]))) [fallsWithin], 
       [Longitude] [longitude], 
       [Latitude] [latitude],
       CASE
           WHEN [Latitude] IS NULL
           THEN NULL -- Taking into account cases where lat and long are not provided, as otherwise conversion
           WHEN [Longitude] IS NULL
           THEN NULL -- to geography data type fails
           WHEN [Latitude] = ''
           THEN NULL -- and converting to geometry data type
           WHEN [Longitude] = ''
           THEN NULL
           ELSE geography::STPointFromText('POINT('+LTRIM(RTRIM(TRY_CAST([Longitude] AS     VARCHAR(15))))+' '+LTRIM(RTRIM(TRY_CAST([Latitude] AS VARCHAR(15))))+')', 4326)
       END [geoLocation], 
       LTRIM(RTRIM([LSOA code])) [lsoaCode],
       CASE
           WHEN LTRIM(RTRIM([Crime Type])) = 'Violent crime'
           THEN 'Violence and sexual offences'
           WHEN LTRIM(RTRIM([Crime type])) = 'Bicycle theft'
           THEN 'Other theft'
           WHEN LTRIM(RTRIM([Crime type])) = 'Theft from the person'
           THEN 'Other theft'
           WHEN LTRIM(RTRIM([Crime type])) = 'Possession of weapons'
           THEN 'Public disorder and weapons'
           WHEN LTRIM(RTRIM([Crime type])) = 'Public order'
           THEN 'Public disorder and weapons'
           ELSE LTRIM(RTRIM([Crime type])) -- Violent crime was renamed to violence and sexual offences in 2013, bicycle theft and theft from the person were taken out of other theft in 2013
       END [crimeType],
	   CASE
		   WHEN [Last outcome category] = ''
		   THEN NULL
		   ELSE LTRIM(RTRIM([Last outcome category]))
	   END;
[outcome];
INTO [dbo].[cleanCrimeData]
FROM newCrimeData;

-- add crimeID
ALTER TABLE cleanCrimeData
ADD [crimeID] INT NOT NULL IDENTITY PRIMARY KEY;

-- create spatial index on cleanCrimeData geolocation column
CREATE SPATIAL INDEX SIndx_SpatialTable_geometry_col1 ON [dbo].[cleanCrimeData]
(geoLocation
);
GO

--------------------------------------------------------------------------
-- Cleaning and combining all weather data
--------------------------------------------------------------------------
-- need to remember that when I find closest station I need to check that it was
-- active at the time of the crime
-- Here I had problems importing the excel files as one table so manually
-- combine and clean the different stations
-- the nullif must come insode the try_cast otherwise errors
-- occur due to trying to cast '---' as a float

IF OBJECT_ID('weatherData') IS NOT NULL
    DROP TABLE dbo.weatherData;
GO

-- union all individually imported weather data tables
-- in a cte in order to one table
WITH cte
     AS (SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[aberporthdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[armaghdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[ballypatrickdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[braemardata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[cambornedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[cambridgedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[cardiffdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[cwmystwythdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[dunstaffnagedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Durham' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[durhamdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[eastbournedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Eskdale' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[eskdalemuirdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[heathrowdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[hurndata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Lerwick' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[lerwickdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[leucharsdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[manstondata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[newtonriggdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Oxford' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[oxforddata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[paisleydata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'RossOnWye' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[rossonwyedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[shawburydata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Sheffield' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[sheffielddata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Stornoway' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[stornowaydata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[suttonboningtondata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Tiree' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[tireedata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Valley' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[valleydata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[waddingtondata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM(replace([F10], '$', ''))), '---') AS FLOAT) [sun] -- this column did not import properly so needed 
         FROM [trash].[whitbydata$]														      -- extra cleansing
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                'Wick Airport' [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[wickairportdata$]
         UNION ALL
         SELECT [Longitude], 
                [Latitude], 
                [Area], 
                [yyyy] [Year], 
                [mm] [Month], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmax])), '---') AS FLOAT) [tmax], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([tmin])), '---') AS FLOAT) [tmin], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([af])), '---') AS FLOAT) [af], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([rain])), '---') AS FLOAT) [rain], 
                TRY_CAST(NULLIF(LTRIM(RTRIM([sun])), '---') AS FLOAT) [sun]
         FROM [trash].[yeoviltondata$])

     -- clean data into one table with lat, long converted into geometry 
     SELECT [Longitude] [longitude], 
            [Latitude] [latitude], 
            geography::STPointFromText('POINT('+LTRIM(RTRIM(TRY_CAST([longitude] AS VARCHAR(15))))+' '+LTRIM(RTRIM(TRY_CAST([latitude] AS VARCHAR(15))))+')', 4326) [geoLocation], 
            [Area] [area], 
            [Year] [year], 
            [Month] [month], 
            [tmax] [maxTemp], 
            [tmin] [minTemp], 
            [af] [airFrostDays], 
            [rain] [rainfall], 
            [sun] [hoursOfSun]
     INTO dbo.weatherData
     FROM cte
     WHERE [year] > 2009
           AND [tmax] IS NOT NULL
           AND [tmin] IS NOT NULL; -- only need recent weather data
GO

--Here I am moving individual tables into a trash schema 
--alter schema trash transfer [dbo].[weatherData]
--alter schema trash transfer [dbo].[aberporthdata$]
--alter schema trash transfer [dbo].[armaghdata$]
--alter schema trash transfer [dbo].[ballypatrickdata$]
--alter schema trash transfer [dbo].[braemardata$]
--alter schema trash transfer [dbo].[cambornedata$]
--alter schema trash transfer [dbo].[cambridgedata$]
--alter schema trash transfer [dbo].[cardiffdata$]
--alter schema trash transfer [dbo].[cwmystwythdata$]
--alter schema trash transfer [dbo].[dunstaffnagedata$]
--alter schema trash transfer [dbo].[durhamdata$]
--alter schema trash transfer [dbo].[eastbournedata$]
--alter schema trash transfer [dbo].[eskdalemuirdata$]
--alter schema trash transfer [dbo].[heathrowdata$]
--alter schema trash transfer [dbo].[hurndata$]
--alter schema trash transfer [dbo].[lerwickdata$]
--alter schema trash transfer [dbo].[leucharsdata$]
--alter schema trash transfer [dbo].[lowestoftdata$]
--alter schema trash transfer [dbo].[manstondata$]
--alter schema trash transfer [dbo].[nairndata$]
--alter schema trash transfer [dbo].[newtonriggdata$]
--alter schema trash transfer [dbo].[oxforddata$]
--alter schema trash transfer [dbo].[paisleydata$]
--alter schema trash transfer [dbo].[ringwaydata$]
--alter schema trash transfer [dbo].[rossonwyedata$]
--alter schema trash transfer [dbo].[shawburydata$]
--alter schema trash transfer [dbo].[sheffielddata$]
--alter schema trash transfer [dbo].[southamptondata$]
--alter schema trash transfer [dbo].[stornowaydata$]
--alter schema trash transfer [dbo].[suttonboningtondata$]
--alter schema trash transfer [dbo].[tireedata$]
--alter schema trash transfer [dbo].[valleydata$]
--alter schema trash transfer [dbo].[waddingtondata$]
--alter schema trash transfer [dbo].[yeoviltondata$]
--alter schema trash transfer [dbo].[whitbydata$]
--alter schema trash transfer [dbo].[wickairportdata$]
------------------------------------------------------------------------------------------------------
-- Cleaning local authority boundary table and adding in population
------------------------------------------------------------------------------------------------------

IF OBJECT_ID('localAuthorityBoundaries') IS NOT NULL
    DROP TABLE localAuthorityBoundaries;

SELECT objectid [ID], 
       LTRIM(RTRIM([lad17cd])) [laCode],
       CASE
           WHEN LTRIM(RTRIM([lad17nm])) = 'Vale of Glamorgan'
           THEN 'The Vale of Glamorgan'
           ELSE LTRIM(RTRIM([lad17nm])) -- renaming vale of glamorgan so that it matches when creating fact table
       END [laName], -- with the intermediary table to preserve all crime data 
       [SpatialObj] [geoBoundaries] -- .shp file imported using alteryx --> allowing geogrpahy datatype instead of geometry
INTO [dbo].[localAuthorityBoundaries] -- Note that Northern Ireland boundary is not present so is excluded when joining the fact table
FROM [dbo].[LABoundary];
GO

-----------------------------------------------------------------------------------------------------
-- Adding table with flood data to see effects of flooding on crime in a radius around flood area
----------------------------------------------------------------------------------------------------

IF OBJECT_ID('floods') IS NOT NULL
    DROP TABLE dbo.floods;

IF OBJECT_ID('floodTable') IS NOT NULL
    DROP TABLE dbo.floodTable;

CREATE TABLE floods
(ID          INT NOT NULL PRIMARY KEY, 
 [location]  VARCHAR(50), 
 [month]     INT, 
 [year]      INT,
 [dateID]    INT, 
 [longitude] FLOAT, 
 [latitude]  FLOAT
);

INSERT INTO floods
VALUES
(1, 
 'Staines', 
 2, 
 2014,
 38, 
 -0.5399857, 
 51.4223626
),
(2, 
 'Bridgewater', 
 1, 
 2014,
 37, 
 -2.942568, 
 51.187486
),
(3, 
 'Galgate', 
 9, 
 2017,
 81, 
 -2.7986906, 
 53.9908008
),
(4, 
 'Malmesbury', 
 9, 
 2012,
 21,
 -2.1164607, 
 51.5869547
);

-- converting flood lat and long to geometry
SELECT *, 
       geography::STPointFromText('POINT('+LTRIM(RTRIM(TRY_CAST([Longitude] AS VARCHAR(15))))+' '+LTRIM(RTRIM(TRY_CAST([Latitude] AS VARCHAR(15))))+')', 4326) [geoLocation]
INTO [dbo].[floodTable]
FROM [dbo].[floods];
GO