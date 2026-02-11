DECLARE @CdtFrom DATETIME;
DECLARE @CdtTo DATETIME;

SELECT @CdtFrom = '2026-01-01 00:00:01.000'; -- set start
SELECT @CdtTo = '2026-02-01 00:00:01.000';   -- set stop

WITH A AS (
SELECT a.Sno,
	   a.Dn,
	   b.Po,
	   b.Status,
	   d.Rank,
	   b.Qty,
	   e.PDName,
	   a.Model,
	   ISNULL(f.EDI850, '-') AS [EDI850],
	   ISNULL(f.EDI855, '-') AS [EDI855],
	   c.Cdt AS [MVS Pass],
	   c.PdLine AS [MVS Pass Line]
FROM CTO..SNO a WITH(INDEX(Idx_Sno_Status))
	LEFT JOIN CTO..PO b ON a.Po = b.Po 
	LEFT JOIN CTO..UNIT_LOG c WITH(INDEX(Idx_Wc_IsPass_Cdt)) ON a.Sno = c.Sno 
	LEFT JOIN CTO..WIS_SEQ d ON d.Po = b.Po
	LEFT JOIN CTO..FAMILY e WITH(INDEX(I_FAMILY)) ON e.Family = a.Family
	LEFT JOIN CTO..Dn_Mas f ON f.Dn = a.Dn
WHERE c.Wc = '40'
	AND c.IsPass = 1
	AND c.Cdt BETWEEN @CdtFrom AND @CdtTo
	AND a.Sno LIKE 'CZU%'
	AND b.Po NOT LIKE 'ICZIT%'
)
SELECT a.*,
	[PO Download].[PO Download], 
	[PO Release].[PO Release],
	[SN Gen].[SN Gen],
	[TravelLabelPrint].[TravelLabelPrint],
	[FinalInspPass].[FinalInspPass],
	[ElapsedTime_MVS_FI (HOURS)].[ElapsedTime_MVS_FI (HOURS)],
	[DIMM Qty].[DIMM Qty],
	[CPU Qty].[CPU Qty],
	[HDD Qty].[HDD Qty],
	[CHIS Qty].[CHIS Pn],
	[CHIS Qty].[CHIS Ct],
	[StartTime].[MVS-1 StartTime],
	[StartTime].[MVS-2 StartTime],
	[StartTime].[MVS-3 StartTime],
	[StartTime].[MVS-4 StartTime],
	[StartTime].[MVS-5 StartTime],
	[StartTime].[MVS-6 StartTime],
	[StartTime].[MVS-7 StartTime],
	[StartTime].[MVS-8 StartTime],
	[StartTime].[MVS-9 StartTime],
	[StartTime].[MVS-10 StartTime],
	[StartTime].[MVS-11 StartTime],
	[StartTime].[MVS-12 StartTime],
	[StartTime].[MVS-13 StartTime],
	[StartTime].[MVS-14 StartTime],
	[StartTime].[MVS-15 StartTime],
	[StartTime].[HIPOT-01 StartTime],
	[StartTime].[COO-01 StartTime],
	[StartTime].[MVS-SY StartTime]
FROM A a

--PO DOWNLOAD TIME
OUTER APPLY (
	SELECT pl.Cdt AS [PO Download] 
	FROM PO_LOG pl WITH (INDEX([I_INDEX1])) 
	WHERE pl.Po = a.Po 
		AND pl.Event = 'M'
) [PO Download]

--PO RELEASE TIME
OUTER APPLY (
	SELECT pl.Cdt AS [PO Release] 
	FROM PO_LOG pl WITH (INDEX([I_INDEX1])) 
	WHERE pl.Po = a.Po 
		AND pl.Event = 'R'
) [PO Release] 

--PO PRINT TC / SN Gen TIME
OUTER APPLY (
	SELECT pl.Cdt AS [SN Gen] 
	FROM PO_LOG pl WITH (INDEX([I_INDEX1])) 
	WHERE pl.Po = a.Po 
		AND pl.Event = 'H'
) [SN Gen]

--Travel Label Print
OUTER APPLY (
	SELECT ll.Cdt AS [TravelLabelPrint]
	FROM LABEL_LOG ll WITh (INDEX([LABEL_SNO]))
	WHERE ll.Label = 'Travel_Label_ICZ' AND ll.Sno = a.Sno
) [TravelLabelPrint]

--Final Inspection Pass
OUTER APPLY (
	SELECT ul.Cdt AS [FinalInspPass] 
	FROM CTO..UNIT_LOG ul WITH(INDEX(Idx_Wc_IsPass_Cdt))
	WHERE ul.Sno = a.Sno AND ul.Wc = '74' AND ul.IsPass = 1
) [FinalInspPass]

--MVS Pass x FI Pass time diff in hours
OUTER APPLY (
	SELECT ABS(DATEDIFF(HOUR,a.[MVS Pass], [FinalInspPass].FinalInspPass)) 
	AS [ElapsedTime_MVS_FI (HOURS)]

) [ElapsedTime_MVS_FI (HOURS)]

--DIMM Qty --NEED OPT
OUTER APPLY (
	SELECT SUM(pb.Qty) AS [DIMM Qty] 
	FROM CTO..POBOM pb 
		LEFT JOIN CTO..PARTS p ON pb.Po = a.Po AND pb.PartNo = p.Pn 
	WHERE p.Category = 'DIMM' 
		AND p.Pn <> 'Z_DIMM_CHECK'
		AND pb.Status = '1' 
		AND pb.Type = 'CT' 
		AND pb.ChkPoint LIKE '1%'
	GROUP BY pb.Po
) [DIMM Qty]

--CPU Qty --NEED OPT
OUTER APPLY (
	SELECT SUM(pb.Qty) AS [CPU Qty] 
	FROM CTO..POBOM pb 
		LEFT JOIN CTO..PARTS p ON pb.Po = a.Po AND pb.PartNo = p.Pn 
	WHERE p.Category = 'CPU' 
		AND pb.Status = '1' 
		AND pb.Type = 'CT' 
		AND pb.ChkPoint LIKE '1%'
		GROUP BY pb.Po
) [CPU Qty]

--HDD Qty --NEED OPT
OUTER APPLY (
	SELECT SUM(pb.Qty) AS [HDD Qty] 
	FROM CTO..POBOM pb 
		LEFT JOIN CTO..PARTS p ON pb.Po = a.Po AND pb.PartNo = p.Pn 
	WHERE p.Category = 'HDD' 
		AND pb.Status = '1' 
		AND pb.Type = 'GN' 
		AND pb.ChkPoint LIKE '1%'
	GROUP BY pb.Po
) [HDD Qty]

--CHIS Qty --NEED OPT
OUTER APPLY (
	SELECT sp.ScanPn AS [CHIS Pn], sp.ScanSn AS [CHIS Ct] FROM CTO..SNO_PARTS sp 
		LEFT JOIN CTO..PARTS p ON sp.ScanPn = p.Pn
	WHERE sp.Sno = a.Sno
		AND sp.Status = '1'
		AND p.Category = 'CHIS'
		AND p.Commodity = 'CHS'
		AND p.PType = 'CT'
) [CHIS Qty]

--Positions Start Times
OUTER APPLY (
    SELECT 
        MAX(CASE WHEN ol.SubWc = 'MVS-1' THEN ol.StartTime END) AS [MVS-1 StartTime],
        MAX(CASE WHEN ol.SubWc = 'MVS-2' THEN ol.StartTime END) AS [MVS-2 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-3' THEN ol.StartTime END) AS [MVS-3 StartTime],
        MAX(CASE WHEN ol.SubWc = 'MVS-4' THEN ol.StartTime END) AS [MVS-4 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-5' THEN ol.StartTime END) AS [MVS-5 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-6' THEN ol.StartTime END) AS [MVS-6 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-7' THEN ol.StartTime END) AS [MVS-7 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-8' THEN ol.StartTime END) AS [MVS-8 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-9' THEN ol.StartTime END) AS [MVS-9 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-10' THEN ol.StartTime END) AS [MVS-10 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-11' THEN ol.StartTime END) AS [MVS-11 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-12' THEN ol.StartTime END) AS [MVS-12 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-13' THEN ol.StartTime END) AS [MVS-13 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-14' THEN ol.StartTime END) AS [MVS-14 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-15' THEN ol.StartTime END) AS [MVS-15 StartTime],
		MAX(CASE WHEN ol.SubWc = 'MVS-SY' THEN ol.StartTime END) AS [MVS-SY StartTime],
		MAX(CASE WHEN ol.SubWc = 'HIPOT-01' THEN ol.StartTime END) AS [HIPOT-01 StartTime],
		MAX(CASE WHEN ol.SubWc = 'FIN.INP-01' THEN ol.StartTime END) AS [FIN.INP-01 StartTime],
		MAX(CASE WHEN ol.SubWc = 'COO-01' THEN ol.StartTime END) AS [COO-01 StartTime]
    FROM OPERATION_LOG ol 
    WHERE ol.Sno = a.Sno 
      AND ol.SubWc IN ('MVS-1', 'MVS-2', 'MVS-3', 'MVS-4', 'MVS-5', 'MVS-6', 'MVS-7', 'MVS-8', 'MVS-9', 'MVS-10', 'MVS-11', 'MVS-22', 'MVS-13', 'MVS-14', 'MVS-15', 'MVS-SY', 'HIPOT-01', 'FIN.INP-01', 'COO-01')
) [StartTime]

ORDER BY Po, Sno, Model
