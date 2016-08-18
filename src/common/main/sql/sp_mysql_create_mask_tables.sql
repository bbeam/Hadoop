--Author: Naveen Rajan
--Version: 1
--Creation Date: July 6, 2016
--Procedure Name: create_mask_tables
--Target Platform: Mysql 5.6.31


--Stored Procedure to create masking tables out of original tables. It takes 2 parameters as input, first one being the input/original/source table, the second one being the masked table which would be created out of the data from the table specified in first parameter 
--Usage set @source_tables='<source/original_tables_delimited_by_comma>';set @masking_tables='<masking_tables_delimited_by_comma>';call create_mask_tables(@source_tables, @masking_tables); The order of parameter values specified for first and second parameter should be relative
--Eg: set @source_tables = 'test1,test2,test3';set @masking_tables = 'test1_mask,test2_mask,test3_mask';call create_mask_tables(@source_tables, @masking_tables);

DELIMITER $$
CREATE PROCEDURE `create_mask_tables`(IN source_tables CHAR(128), IN masking_tables CHAR(128))
BEGIN
    SET @i=1;
    SET @src_lent  =  LENGTH(@source_tables) - LENGTH(REPLACE(@source_tables, ',', '')) + 1 ;
	SET @mask_lent =  LENGTH(@masking_tables) - LENGTH(REPLACE(@masking_tables, ',', '')) + 1 ;
    WHILE @i < @src_lent + 1 DO
    SET @src_tbl = SUBSTRING_INDEX(SUBSTRING_INDEX(@source_tables,',',@i),',',-1) ;
	SET @mask_tbl = SUBSTRING_INDEX(SUBSTRING_INDEX(@masking_tables,',',@i),',',-1) ;
    SET @c = CONCAT(' CREATE TABLE ',@mask_tbl,' as SELECT * FROM ',@src_tbl );
    PREPARE stmt FROM @c;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @i=@i+1;
    END WHILE;
    END$$
DELIMITER ;

