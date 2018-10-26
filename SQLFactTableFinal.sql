USE crime;
GO

-- I needed to use some intermediary table to join lsoacode of each crime
-- to region codes as it was much faster than using the STIntersect method
-- However this created some loss so decided to run with intersect method
-- using a spatial index which cut the run time down to 1 hour
IF OBJECT_ID('dbo.factTable') IS NOT NULL
    DROP TABLE dbo.factTable;

-- creating fact table
SELECT c.crimeID, 
       d.dateID, 
       r.reportedbyID, 
       f.fallswithinID, 
       t.crimetypeID, 
       b.boundaryID, 
       w.weatherID, 
       c.latitude, 
       c.longitude, 
       c.geoLocation,
	   o.outcomeID
INTO dbo.factTable
FROM dbo.cleanCrimeData c WITH (INDEX(SIndx_SpatialTable_geometry_col1))
     INNER JOIN [dbo].[dateDIM] d ON c.[month] = d.[month]
                                     AND c.[year] = d.[year]
     INNER JOIN [dbo].[reportedbyDIM] r ON c.reportedBy = r.reportedBy
     INNER JOIN [dbo].[fallswithinDIM] f ON c.fallsWithin = f.fallswithin
     INNER JOIN [dbo].[crimetypeDIM] t ON c.crimeType = t.crimeType
     ------------------------------------------------------------------------------------------------
     -- This was the first faster method but some data loss occurred
	 -- inner join [dbo].[lsoa2la] l
     --	on c.lsoaCode = l.[lsoacode] -- need an intermediate table to connect lsoa 
     -- inner join [dbo].[boundaryDIM] b -- to local authority as using an intersect
     --	on ltrim(rtrim(l.[name])) = b.[laName] -- with polygon and crime location is inefficient
     ------------------------------------------------------------------------------------------------
     INNER JOIN [dbo].[boundaryDIM] b -- some crimes had no lsoa code from the british transport police 
     ON c.geoLocation.STIntersects(b.geoBoundaries) = 1 -- so I have tried to join on intersection of /
     -- Spatial index hint is now on the crime location geometry for faster intersection lookup
     -- This join filters out N Ireland
     -------------------------------------------------------------------------------------------------
     -------------------------------------------------------------------------------------------------
     INNER JOIN [dbo].[weatherdataDIM] w 
	 ON b.boundaryID = w.boundaryID
		AND d.dateID = w.dateID;
	 LEFT JOIN [dbo].[outcomeDIM] o -- left join as many outcomes are null
	 ON c.outcome = o.outcome


------------------------------------------------------------------------------------------------------
-- Creating all foreign keys
------------------------------------------------------------------------------------------------------
ALTER TABLE factTable
ADD CONSTRAINT fk_outcomeID FOREIGN KEY(outcomeID) REFERENCES outcomeDIM(outcomeID);
ALTER TABLE factTable
ADD CONSTRAINT fk_boundaryID FOREIGN KEY(boundaryID) REFERENCES boundaryDIM(boundaryID);
ALTER TABLE factTable
ADD CONSTRAINT fk_crimeTypeID FOREIGN KEY(crimetypeID) REFERENCES crimetypeDIM(crimetypeID);
ALTER TABLE factTable
ADD CONSTRAINT fk_dateID FOREIGN KEY(dateID) REFERENCES dateDIM(dateID);
ALTER TABLE factTable
ADD CONSTRAINT fk_fallswithinID FOREIGN KEY(fallswithinID) REFERENCES fallswithinDIM(fallswithinID);
ALTER TABLE factTable
ADD CONSTRAINT fk_reportedbyID FOREIGN KEY(reportedbyID) REFERENCES reportedbyDIM(reportedbyID);
ALTER TABLE factTable
ADD CONSTRAINT fk_weatherdataID FOREIGN KEY(weatherID) REFERENCES weatherdataDIM(weatherID);
ALTER TABLE factTable
ADD CONSTRAINT fk_outcomeID FOREIGN KEY(outcomeID) REFERENCES outcomeDIM(outcomeID);
ALTER TABLE floodTable
ADD CONSTRAINT fk_floodID FOREIGN KEY(dateID) REFERENCES dateDIM(dateID);