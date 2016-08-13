CREATE  PROCEDURE mask_table_md5( @tbl_to_mask NVARCHAR(128),  @cols NVARCHAR(128))
AS
BEGIN
  DECLARE @i int
  DECLARE @s_x int
  DECLARE @s_y int
  DECLARE @cols_lent VARCHAR(256)
  DECLARE @cols_concat VARCHAR(256)
  DECLARE @single_col VARCHAR(256)
  DECLARE @updtae_stmt VARCHAR(1000)
  DECLARE @alter_stmt1 VARCHAR(1000)
  DECLARE @alter_stmt2 VARCHAR(1000)
  SET @i = 1
  SET @s_y = 1
  SET @cols_lent  =  LEN(@cols) - LEN(REPLACE(@cols, ',', '')) + 1 
  SET @cols_concat = CONCAT(@cols,',')
  WHILE @i < @cols_lent + 1 
  BEGIN

      SET @s_x=CHARINDEX(',',@cols_concat,1) -1
      SET @single_col = SUBSTRING(@cols_concat,1,@s_x)
      SET @cols_concat = SUBSTRING(@cols_concat,@s_x+2,len(@cols_concat))
      SET @s_y=@s_x + 1
	  SET @alter_stmt1=CONCAT('ALTER TABLE ',@tbl_to_mask )
	  SET @alter_stmt2=CONCAT('AlTER COLUMN ',@single_col,' varchar(1000)')
	  PRINT (@alter_stmt1)
	  PRINT (@alter_stmt2)
   
       IF  @i > 1
	  SET @updtae_stmt=CONCAT(@updtae_stmt,',',@single_col,'=CONVERT(VARCHAR(32),HashBytes(''MD5'',','ISNULL(' ,@single_col,',','''NULL''',')),2)')
      ELSE
	  SET @updtae_stmt=CONCAT('UPDATE ',@tbl_to_mask,' SET ',@single_col,'=CONVERT(VARCHAR(32),HashBytes(''MD5'',','ISNULL(' ,@single_col,',','''NULL''',')),2)')
	  
      print @updtae_stmt
      SET @i= @i + 1
 END 
       exec(@updtae_stmt)

END