--DROP Tables
DROP TABLE IF EXISTS #temp_WC;
DROP TABLE IF EXISTS #temp_Data;
DROP TABLE IF EXISTS #temp_Result;
DROP TABLE IF EXISTS #temp_WC_clean;

--GET Stations
--SELECT DISTINCT value, 
--       DENSE_RANK()OVER(ORDER BY value) AS [rn], 
--       Description 
--INTO #temp_WC 
--FROM STRING_SPLIT((SELECT STRING_AGG(Flow, '/') 
--FROM (SELECT DISTINCT Flow FROM SFC WHERE Flow <> '' AND Cdt >= '20250101') AS Flow),'/')
--LEFT JOIN CTO..WC ON value = WC.WC
--WHERE value <> '' 
--AND value IN (select WC from CTO..WC where WC = value);

--GET Stations
SELECT DISTINCT s.value 
INTO #temp_WC_clean  
FROM (
    SELECT DISTINCT a.Flow FROM CTO..SFC a WHERE a.Flow <> '' AND a.Cdt >= '20250101'
) AS flow
CROSS APPLY STRING_SPLIT(flow.Flow, '/') AS s
WHERE s.value <> '';

SELECT a.value,
       w.Description
INTO #temp_WC
FROM #temp_WC_clean a 
JOIN CTO..WC w ON a.value = w.WC;

--DELETE Unwanted stations from #temp_WC
DELETE FROM #temp_WC 
WHERE value IN ('AC','78','9C','78','69');

--GET Stations Description for column names
DECLARE @cols NVARCHAR(MAX);
SELECT @cols = STRING_AGG('ISNULL(CAST(' + QUOTENAME(t.value) + ' AS VARCHAR(MAX)), ''-'') AS ' + QUOTENAME(t.Description), ', ') 
FROM #temp_WC t;

--GET Units
WITH Units AS (
SELECT a.Sno, 
       a.Po, 
       CASE WHEN b.Status = 'M' THEN 'Initial'
            WHEN b.Status = 'R' THEN 'Release'
            WHEN b.Status = 'H' THEN 'SN Generated'
            WHEN b.Status = 'O' THEN 'Openned'
            WHEN b.Status = 'S' THEN '1. Units Passed COO'
            WHEN b.Status = 'C' THEN 'Closed'
            WHEN b.Status = 'D' THEN 'Deleted'
            WHEN b.Status = 'B' THEN 'Blocked'
            WHEN b.Status = 'F' THEN 'Confirmation'
       END AS [Status], 
       a.Model
FROM CTO..SNO a 
LEFT JOIN CTO..PO b ON a.Po = b.Po 
LEFT JOIN CTO..PO_LOG c WITH(INDEX(I_INDEX1)) ON a.Po = c.Po AND b.Po = c.Po
WHERE c.Cdt >= DATEADD(DAY,-7,GETDATE())
 AND c.Event = 'R' 
), PassStation AS (
    SELECT a.Sno, 
           a.Po, 
           a.Status, 
           a.Model, 
           b.Wc, 
           b.Cdt 
    FROM Units a 
    LEFT JOIN CTO..UNIT_LOG b ON a.Sno = b.Sno AND b.Wc IN (SELECT value FROM #temp_WC)
)
SELECT * INTO #temp_Data FROM PassStation;

--Pivot #temp_Data
DECLARE @DynamicSQL NVARCHAR(MAX);
SET @DynamicSQL = '
SELECT Sno, 
       Po, 
       Status, 
       Model, 
       ' + @cols + ' 
INTO #temp_Result
FROM (SELECT Sno, 
             Po, 
             Status, 
             Model, 
             Wc, 
             Cdt 
       FROM #temp_Data) up 
PIVOT (MAX(Cdt) FOR Wc IN (' + (SELECT STRING_AGG(QUOTENAME(value), ',') FROM #temp_WC) + ')) 
AS pvt;

SELECT ISNULL(CAST([LinePosition].[LinePosition] AS VARCHAR), ''-'') AS [Position / Line],
       a.*, 
       ISNULL(CAST([CAS].[CAS] AS VARCHAR), ''-'') AS [CAS], 
       ISNULL(CAST([OOBA].[OOBA] AS VARCHAR), ''-'') AS [OOBA], 
       ISNULL(CAST([PIA].[PIA] AS VARCHAR),''-'') AS [PIA], 
       ISNULL(CAST([Error].[Error / Position] AS VARCHAR), ''-'') AS [Error / Position], 
       ISNULL(CAST([Ship].[DN] AS VARCHAR), ''-'') AS [DN], 
       ISNULL(CAST([Ship].[Invoice] AS VARCHAR), ''-'') AS [Invoice], 
       ISNULL(CAST([Ship].[Sono] AS VARCHAR),''-'') AS [Sono],
       ISNULL(CAST([05Scan].[05Scan] AS VARCHAR), ''-'') AS [05Scan], 
       ISNULL(CAST([15Scan].[15Scan] AS VARCHAR), ''-'') AS [15Scan], 
       ISNULL(CAST([Cobol].[Cobol] AS VARCHAR), ''-'') AS [Cobol],
       ISNULL(CAST([17Scan].[17Scan] AS VARCHAR), ''-'') AS [17Scan], 
       ISNULL(CAST([20Scan].[20Scan] AS VARCHAR), ''-'') AS [20Scan], 
       ISNULL(CAST([PGI].[PGI] AS VARCHAR), ''-'') AS [PGI]
FROM #temp_Result a

--Last error in tests
OUTER APPLY (
    SELECT TOP 1 b.Data + '' / '' + (SELECT wc.Description FROM CTO..WC wc WHERE wc.WC = b.Wc ) AS [Error / Position] 
    FROM CTO..UNIT_TESTING_DATA b 
    WHERE b.Status = ''F'' 
     AND b.Attribute = ''DefectCode'' 
     AND b.Sno = a.Sno 
    ORDER BY b.Cdt DESC
) AS Error

--Ship / DN
OUTER APPLY (
    SELECT TOP 1 c.DN AS [DN], d.[Invoice], d.[Sono]
    FROM CTO..Shipping_Flow_Log c 
    LEFT JOIN CTO..DN d ON c.DN = d.Dn
    WHERE c.SN = a.Sno 
     AND d.Dn = c.DN
) AS [Ship]

--PGI
OUTER APPLY (
    SELECT TOP 1 e.Cdt AS [PGI] 
    FROM CTO..DN_LOG e 
    WHERE e.Dn = Ship.DN 
     AND Event = ''P''
) AS [PGI]

--15Scan
OUTER APPLY (
    SELECT TOP 1 e.Cdt AS [15Scan] 
    FROM CTO..DN_LOG e 
    WHERE e.Dn = Ship.DN 
     AND e.Event = ''V''
) AS [15Scan]

--20Scan
OUTER APPLY (
    SELECT TOP 1 f.Cdt AS [20Scan] 
    FROM CTO..DN_LOG f 
    WHERE f.Dn = Ship.DN 
     AND f.Event = ''Z''
) AS [20Scan]

--Line Status
OUTER APPLY (
    SELECT TOP 1 SubWc + '' / '' + PdLine AS [LinePosition], 
    DENSE_RANK()OVER(PARTITION BY g.Sno, g.SubWc ORDER BY g.SubWc) AS [rn]  
    FROM CTO..SNO_PARTS g 
    WHERE g.Status = ''1''
     AND g.Sno = a.Sno
    GROUP BY g.Sno, g.SubWc, g.PdLine
) AS [LinePosition]

--CAS
OUTER APPLY (
    SELECT TOP 1 h.Cdt AS [CAS] 
    FROM CTO..QA_INSP h 
    WHERE h.Sno = a.Sno 
     AND h.Status = ''1'' 
     AND h.Wc = ''CAS''
) AS [CAS]

--OOBA
OUTER APPLY (
    SELECT TOP 1 i.Cdt AS [OOBA] 
    FROM CTO..QA_INSP i 
    WHERE i.Sno = a.Sno 
     AND i.Status = ''1'' 
     AND i.Wc = ''75''
) AS [OOBA]

--PIA
OUTER APPLY (
    SELECT TOP 1 j.Cdt AS [PIA] 
    FROM CTO..QA_INSP j 
    WHERE j.Sno = a.Sno 
     AND j.Status = ''1'' 
     AND j.Wc = ''75''
) AS [PIA]

--17Scan
OUTER APPLY (
    SELECT TOP 1 k.Cdt AS [17Scan]
    FROM CTO..Shipping_Flow_Log k 
    WHERE k.DN = Ship.DN 
     AND k.Station = ''17'' 
     AND k.Type = ''DN''
) AS [17Scan]

--Cobol
OUTER APPLY(
    SELECT TOP 1 l.Cobol AS [Cobol]
    FROM CTO..COBOL_DATA l 
    WHERE l.Misid = Ship.DN 
) AS [Cobol]

--05Scan
OUTER APPLY (
    SELECT TOP 1 m.Cdt AS [05Scan]
    FROM CTO..Shipping_Flow_Log m 
    WHERE m.DN = Ship.DN 
     AND m.Station = ''05'' 
     AND m.Type = ''DN''
) AS [05Scan]

';

EXEC sp_executesql @DynamicSQL;





