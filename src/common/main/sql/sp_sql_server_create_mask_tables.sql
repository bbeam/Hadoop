CREATE  PROCEDURE create_mask_tables( @source_tables NVARCHAR(128),  @masking_tables NVARCHAR(128))
AS
BEGIN
  DECLARE @i int
  DECLARE @s_x int
  DECLARE @s_y int
  DECLARE @m_x int
  DECLARE @m_y int 
  DECLARE @src_lent VARCHAR(256)
  DECLARE @mask_lent VARCHAR(256)
  DECLARE @source VARCHAR(256)
  DECLARE @masking VARCHAR(256)
  DECLARE @single_source_tab VARCHAR(256)
  DECLARE @single_mask_tab VARCHAR(256) 
  DECLARE @sql_stmt VARCHAR(256)
  SET @i = 1
  SET @s_y = 1
  SET @m_y = 1
    SET @src_lent  =  LEN(@source_tables) - LEN(REPLACE(@source_tables, ',', '')) + 1 
    SET @mask_lent =  LEN(@masking_tables) - LEN(REPLACE(@masking_tables, ',', '')) + 1 
    SET @source = CONCAT(@source_tables,',')
    SET @masking = CONCAT(@masking_tables,',')
  WHILE @i < @src_lent + 1 
  BEGIN

SET @s_x=CHARINDEX(',',@source,1) -1
SET @single_source_tab = SUBSTRING(@source,1,@s_x)

SET @source = SUBSTRING(@source,@s_x+2,len(@source))
SET @s_y=@s_x + 1

SET @m_x=CHARINDEX(',',@masking,1) -1
SET @single_mask_tab = SUBSTRING(@masking,1,@m_x)

SET @masking = SUBSTRING(@masking,@m_x+2,len(@masking))
SET @m_y=@m_x + 1

SET @sql_stmt = CONCAT(' SELECT * INTO ',@single_mask_tab,' FROM ',@single_source_tab )
print @sql_stmt
exec(@sql_stmt)
SET @i= @i + 1
END 
END