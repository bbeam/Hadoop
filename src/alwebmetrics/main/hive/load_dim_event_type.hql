-- ######################################################################################################### 
-- HIVE SCRIPT				:load_dim_event_type.hql
-- AUTHOR					:Gaurav Maheshwari.
-- DESCRIPTION				:This script will load the data into table dim_event_type
-- #########################################################################################################


use ${hivevar:GOLD_AL_WEB_METRICS};

-- =========Manual insert statement to load the data into target table========

INSERT INTO TABLE dim_event_type VALUES
(1,"Login-Web","Not Applicable",0,"web"),
(2,"Login-Web-Successful","Not Applicable",0,"web"),
(3,"Login-Mobile","Not Applicable",1,"ios"),
(4,"Login-Mobile","Not Applicable",1,"android"),
(5,"Storefront Item View","Not Applicable",0,"web"),
(6,"Profile View","Not Applicable",0,"web"),
(7,"Review Submitted","Not Applicable",0,"web"),
(8,"View","Not Applicable",0,"web"),
(9,"Add to Cart","Not Applicable",0,"web"),
(10,"Purchase","Not Applicable",0,"web"),
(11,"Offer Page Loaded","Not Applicable",0,"web"),
(12,"Writes an SP Review","Not Applicable",1,"ios"),
(13,"Review Submitted","Not Applicable",1,"android"),
(14,"Popular Category Tapped","Not Applicable",1,"android"),
(15,"STL Search","Search The List",1,"android"),
(16,"Search the List","Search The List",1,"ios"),
(17,"Searched SP","SP Search - category",0,"web"),
(18,"Searched SP","SP Search - service_provider",0,"web"),
(19,"Searched SP","SP Search - keyword",0,"web"),
(20,"Legacy Ecommerce Purchase","Not Applicable",0,"web"),
(21,"Purchased Cart","Not Applicable",0,"web"),
(22,"Purchased an Item","Not Applicable",0,"web"),
(23,"Purchased","Not Applicable",0,"web"),
(24,"Store Page Loaded","Not Applicable",0,"web"),
(25,"Mobile Activity via Web Request","Not Applicable",0,"web"),
(26,"Web Segment Page Load","Not Applicable",0,"web"),
(27,"Android Segment Screen Load","Not Applicable",1,"android"),
(28,"iOS Segment Screen Load","Not Applicable",1,"ios"),
(29,"BigDeal Feedback Interstitial Page","Not Applicable",0,"web"),
(30,"Blended Request","Not Applicable",0,"web"),
(31,"Category Request","Not Applicable",0,"web"),
(32,"Coupon Request","Not Applicable",0,"web"),
(33,"Keyword Request","Not Applicable",0,"web"),
(34,"KeywordBought Request","Not Applicable",0,"web"),
(35,"MyAngie Request","Not Applicable",0,"web"),
(36,"Quick Quote Request","Not Applicable",0,"web"),
(37,"Service Provider Request","Not Applicable",0,"web"),
(38,"Submitted as Report","Not Applicable",0,"web"),
(-1,"Missing","Missing",-1,"Missing"),
(-2,"Not Applicable","Not Applicable",-2,"Not Applicable"),
(-3,"Unknown","Unknown",-3,"Unknown");