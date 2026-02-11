ALTER PROCEDURE ICZ_SEND_MAIL(
	@TableName NVARCHAR (100),
    @Subject NVARCHAR (155),
    @Recipients NVARCHAR (255),
    @Profile_name NVARCHAR (50),
    @Format NVARCHAR (10)
)
AS
BEGIN

--VARIABLES DECLARING
DECLARE @Column NVARCHAR(MAX),
        @ColumnName NVARCHAR(MAX),
        @DynamicSQL NVARCHAR(MAX),
        @TableHTML NVARCHAR(MAX),
        @DateTime NVARCHAR (100),
        @RowCount NVARCHAR (100),
        @DropTempTable NVARCHAR (100),
        @LogQuery NVARCHAR (MAX);

--GET DATE / INIT @DateTime
SELECT @DateTime = CONVERT(NVARCHAR, GETDATE(), 20);

--GET Column names and Columns / INIT @Column AND @ColumnName
SELECT 
    @Column = STRING_AGG(' ''<td>'' + ISNULL(CAST(' + QUOTENAME(name) + ' AS NVARCHAR(MAX)), '''') + ''</td>'' ', ' + '), 
    @ColumnName = STRING_AGG('<th>' + name + '</th>', '')
FROM tempdb.sys.columns 
WHERE object_id = OBJECT_ID('tempdb..' + @TableName);

--DROP temp TABLE ##icz_send_mail_temp BEFORE ITS INIT 
DROP TABLE IF EXISTS ##icz_send_mail_temp;

--CREATE temp TABLE ##icz_send_mail_temp AND LOAD DATA FROM GIVEN TABLE / INIT @DynamicSQL
SET @DynamicSQL = N'SELECT * INTO ##icz_send_mail_temp FROM tempdb..' + @TableName;

--EXECUTE DYNAMIC QUERY @DynamicSQL
EXEC sp_executesql @DynamicSQL;

--GET ROW COUNT FROM TABLE ##icz_send_mail_temp / INIT @RowCount AND CAST TO PROPER DATA TYPE 
SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR (100)) FROM ##icz_send_mail_temp;

--HTML AND CSS INIT
SET @TableHTML = '
    SET @htmlOut = 
    (
        SELECT 
            ''<!DOCTYPE html>
            <html>
            <head><meta charset="UTF-8"></head>
            
            <style>
                body {
	                font-family: "Segoe UI", Arial, sans-serif;
	                margin: 0;
	                padding: 30px;
	                background: linear-gradient(180deg,#1e1e1e,#2c2c2c);
	                color: #f5f5f5;
                }
            
                table {
	                border-collapse: collapse;
	                width: 100%;
	                border-radius: 12px;
	                overflow: hidden;
	                background: #1f2937;
	                box-shadow: 0 6px 20px rgba(0,0,0,0.3);
                }
            
                tr {
                    border-top: none;
                    border-bottom: 1PX SOLID rgb(180, 179, 179);
                    text-align: left ;
                }
            
                th, td {
                    padding: 12px 16px;
	                text-align: left;
                }

                th {
	                background: #374151;
	                color: #f9fafb;
	                font-weight: 600;
	                text-transform: uppercase;
	                font-size: 13px;
	                letter-spacing: .05em;
	            }
            
                .rows-count {
                    text-align: left;
                    padding: 10px;
                    font-weight: bold;
                    border: none;
                    color: rgb(173, 173, 173);
                }
            
                #tr-rows-count{
                    border: none;
                    pointer-events: none;
                }

                .td-no-hoover {
                    background-color: #374151;
                }
            
                tr:hover {
                    background: #2563eb;
	                color: #fff;
                }

                tr:nth-child(even) {
	                background: #111827;
	            }

	            tr:hover td {
	                background: #2563eb;
	                color: #fff;
	            }
            
                #heart {
                    color: red;
                }
            
                #logo
                {
                    font-size: 20px;
	                font-weight: 800;
	                text-align: center;
	                margin: 0;
	                background: linear-gradient(90deg, #00d4ff, #3b82f6, #7c3aed);
	                -webkit-background-clip: text;
	                color: red;
	                text-shadow: 2px 2px 6px rgba(0,0,0,0.6);
                }
            
                .footer {
                    margin-top: 20px;
                    font-size: 0.9em;
                    color: rgb(173, 173, 173);
                    text-align: center ;
                }
            
                #title {
                    text-align: center ;
                    margin-bottom: 20px;
                }
            </style>

            <body>
                <h1 id="title" style="color: white;">' + @Subject + '</h1>
                <table>
                    <tr id="tr-rows-count">
                        <td class="td-no-hoover" colspan="2" id="rows-count">Row Count: ' + @RowCount + '</td>
                    <tr>

                    <tr>
                        '+ @ColumnName +'
                    </tr>
                '' + 
                ISNULL((
                    SELECT 
                        ''<tr>'' 
                            +  ' + @Column + ' + 
                        ''</tr>''
                    FROM ##icz_send_mail_temp
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), '''') + 
                ''</table>

             <div class="footer">
                <span>This mail was generated by FIS System <span id="heart">&#10084;</span>
                <h3> <span id="logo">Inventec</span> - ' + @Subject + ' </h3>
                <span id="footer-time">' + @DateTime +'</span>
                <h5>If you have trouble to display this email please use online version of MS Outlook</h5>
            </div>

            </body>
            </html>''
    )';

--EXECUTE DYNAMIC QUERY @tableHTML
EXEC sp_executesql @TableHTML, N'@htmlOut NVARCHAR(MAX) OUTPUT', @htmlOut = @TableHTML OUTPUT;


---LOGGING START---
DECLARE @SourceTable NVARCHAR(255) = 'tempdb..##icz_send_mail_temp';
DECLARE @LogTable NVARCHAR(255) = 'ICZ_LOCAL..icz_internal_Sent_Data_Over_Mail'; 
DECLARE @Columns NVARCHAR(MAX);
DECLARE @Sql NVARCHAR(MAX);

WITH ColSource AS (
    SELECT name, ROW_NUMBER() OVER (ORDER BY column_id) as rn
    FROM tempdb.sys.columns 
    WHERE object_id = OBJECT_ID(@SourceTable)
),
FiveRows AS (
    SELECT n FROM (VALUES (1),(2),(3),(4),(5)) AS T(n)
)
SELECT @Columns = STRING_AGG(
    ISNULL(QUOTENAME(c.name), ''''''), 
    ', '
) WITHIN GROUP (ORDER BY f.n)
FROM FiveRows f
LEFT JOIN ColSource c ON f.n = c.rn;

SET @Sql = '
INSERT INTO ' + @LogTable + ' (
    Data1, 
    Data2, 
    Data3, 
    Data4, 
    Data5, 
    Mail_Subject, 
    Mail_Body,
    Cdt
)
SELECT 
    ' + @Columns + ', 
    @Subject, 
    @Body,
    @Timestamp
FROM ' + @SourceTable;

EXEC sp_executesql @Sql, 
    N'@Subject NVARCHAR(155), @Body NVARCHAR(MAX), @Timestamp DATETIME',
    @Subject = @Subject, 
    @Body = @TableHTML, 
    @Timestamp = @DateTime;
---LOGGING END---

--DROP GIVEN temp TABLE
SET @DropTempTable = N'DROP TABLE IF EXISTS tempdb..' + @TableName;

--EXECUTE DYNAMIC QUERY @DropTempTable
EXEC sp_executesql @DropTempTable;

--EXECUTE MAIL SEND
EXEC msdb.dbo.sp_send_dbmail 
    @profile_name = @Profile_name,
    @recipients = @Recipients,
    @subject = @Subject,  
    @body = @TableHTML,  
    @body_format = @Format;
END
