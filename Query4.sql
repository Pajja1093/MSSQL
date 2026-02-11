--NEW
WITH A AS (
    SELECT a.Model,
           a.Family,
           a.Sfc,
           a.Cdt
    FROM
        (
            SELECT a.Model, 
                   a.Family, 
                   a.Sfc,
                   COUNT(b.Model) AS [MB ROWS],
                   a.Cdt
            FROM CTO..MODEL a 
            LEFT JOIN CTO..MODELBOM b ON a.Model = b.Model
            WHERE (
                a.Model LIKE 'WG3379%' OR 
                a.Model LIKE 'WG3382%' OR 
                a.Model LIKE 'WG3575%' OR
                a.Model LIKE 'WG3576%' OR
                a.Model LIKE 'WG3650%' OR
                a.Model LIKE 'WG3386%' OR
                a.Model LIKE 'WG3654%' OR
                a.Model LIKE 'WG3577%' OR 
                a.Model LIKE 'WG3485%' OR
                a.Model LIKE 'WG2497%' ) 
                AND a.Cdt >= '20230101'
             GROUP BY a.Model, 
                      a.Family, 
                      a.Sfc, 
                      a.Cdt
             HAVING COUNT(b.Model) > 0
        ) a
), B AS (
    SELECT a.Model,
           COUNT(d.Pn) AS [Count],
           a.Cdt
    FROM A a 
        LEFT JOIN CTO..MODELBOM b WITH (INDEX(Idx_MODELBOM_Model_Pn)) ON a.Model = b.Model 
        LEFT JOIN CTO..MODELBOM c ON c.Model = b.Pn
        LEFT JOIN CTO..PARTS d ON c.Pn = d.Pn 
            AND d.Category = 'MLB' AND d.Commodity = 'MB'
    GROUP BY a.Model, 
             a.Cdt
)
SELECT b.Model AS [Model],
       b.[Count] AS [MLB Count],
       b.Cdt AS [Date of Creation],
       ISNULL([CustomerSo].[CustomerSO], '-') AS [CustomerSO], 
       ISNULL([PO].[Po], '-') AS [PO],
       ISNULL([PO].[Status], '-') AS [Status],
       ISNULL([SEQ].[Seqno], '-') AS [SEQ]
FROM B b 

OUTER APPLY (
    SELECT a.CustomerSO
    FROM CTO..DN a 
    WHERE a.Model = b.Model
    UNION
    SELECT a.CustomerSO
    FROM CTO_History..DN a 
    WHERE a.Model = b.Model
) AS [CustomerSo]

OUTER APPLY (
    SELECT a.Po, 
           CASE WHEN a.Status = 'M' THEN 'Init'
                WHEN a.Status = 'R' THEN 'Release'
                WHEN a.Status = 'H' THEN 'SN Generated'
                WHEN a.Status = 'O' THEN '1. Unit Passed MVS'
                WHEN a.Status = 'S' THEN '1. Unit Passed COO'
                WHEN a.Status = 'C' THEN 'Close'
                WHEN a.Status = 'D' THEN 'Delete'
                WHEN a.Status = 'B' THEN 'Block / Hold'
                ELSE a.Status
            END AS [Status]
    FROM CTO..PO a 
    WHERE a.Model = b.Model
    UNION 
    SELECT a.Po,
          CASE WHEN a.Status = 'M' THEN 'Init'
               WHEN a.Status = 'R' THEN 'Release'
               WHEN a.Status = 'H' THEN 'SN Generated'
               WHEN a.Status = 'O' THEN '1. Unit Passed MVS'
               WHEN a.Status = 'S' THEN '1. Unit Passed COO'
               WHEN a.Status = 'C' THEN 'Close'
               WHEN a.Status = 'D' THEN 'Delete'
               WHEN a.Status = 'B' THEN 'Block / Hold'
               ELSE a.Status
           END AS [Status]
    FROM CTO_History..PO a 
    WHERE a.Model = b.Model
) AS [PO]

OUTER APPLY (
    SELECT a.Seqno
    FROM CTO..WIS_SEQ a 
    WHERE a.Po = [PO].Po
    UNION
    SELECT a.Seqno
    FROM CTO_History..WIS_SEQ a 
    WHERE a.Po = [PO].Po
) AS [SEQ]

WHERE Count <> 1 
ORDER BY b.Cdt DESC;

