--Author: Naveen Rajan
--Version: 1
--Creation Date: July 6, 2016
--Procedure Name: mask_table_md5
--Target Platform: Mysql 5.6.31



--Stored Procedure To Mask Tables, It takes 2 parameters as input, first one being the table to be masked,the second one being the column(s) to be masked 
--Usage: set @tbl_to_mask='<table_to_be_masked>';set @cols='<columns_to_be_masked_delimited_by_comma>';call mask_table_md5(@tbl_to_mask,@cols);
--Eg: set @tbl_to_mask = 'test6_mask';set @cols = 'col1,col2,col3';call mask_table_md5(@tbl_to_mask, @cols);

DELIMITER $$
CREATE PROCEDURE `mask_table_md5`(IN tbl_to_mask CHAR(128), IN cols CHAR(128))
BEGIN
    SET @i=1;
    SET @lent  =  LENGTH(@cols) - LENGTH(REPLACE(@cols, ',', '')) + 1 ;
    WHILE @i < @lent + 1 DO
    SET @maskcol = SUBSTRING_INDEX(SUBSTRING_INDEX(@cols,',',@i),',',-1) ;
    SET @u = CONCAT(' UPDATE ',@tbl_to_mask,' SET ',@maskcol,' = md5(',@maskcol,') ' );
    PREPARE stmt FROM @u;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @i=@i+1;
    END WHILE;
    END$$
DELIMITER ;