DECLARE @Email NVARCHAR(155), @Name NVARCHAR(100);
DECLARE @Host NVARCHAR(100) = HOST_NAME();

SELECT @Email = Email, @Name = Name FROM CTO..Account
WHERE Account = SUBSTRING(@Host, 4, CHARINDEX('-', @Host) - 4) + 
                CAST(CAST(LEFT(SUBSTRING(@Host, 4, CHARINDEX('-', @Host) - 4), 1) AS INT) + 
                CAST(RIGHT(SUBSTRING(@Host, 4, CHARINDEX('-', @Host) - 4), 1) AS INT) AS VARCHAR);

DECLARE @TempTable NVARCHAR(128) = '##tempTable_' + REPLACE(@Host, '-', '_');

DECLARE @Dynamic NVARCHAR(MAX);
SET @Dynamic = '
SELECT ''Hodnota1'' AS MujSloupec, ''Test'' AS MujSloupec2
INTO ' + @TempTable + '
UNION ALL
SELECT ''Hodnota2'', ''Test2'';	

EXECUTE ICZ_SEND_MAIL @TableName = ''' + @TempTable + ''', 
                      @Subject = ''' + @Host + ' ' + ISNULL(@Name, '') + ''',
                      @Recipients = ''' + ISNULL(@Email, '') + ''',
                      @Profile_name = ''ICZ_ITFIS'',
                      @Format = ''HTML'';
';

EXEC sp_executesql @Dynamic;
