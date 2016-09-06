-- ######################################################################################################### 
-- HIVE SCRIPT				:load_dim_event_type.hql
-- AUTHOR					:Gaurav Maheshwari.
-- DESCRIPTION				:This script will load the data into table dim_event_type
-- #########################################################################################################


use ${hivevar:GOLD_AL_WEB_METRICS};

-- =========Manual insert statement to load the data into target table========

INSERT INTO TABLE dim_event_type VALUES
(1,"Login-Web","Not Applicable",0,"web"),
(2,"Login-Mobile","Not Applicable",1,"ios"),
(3,"Login-Mobile","Not Applicable",1,"android"),
(4,"Storefront Item View","Not Applicable",0,"web"),
(5,"Profile View","Not Applicable",0,"web"),
(6,"Review Submitted","Not Applicable",0,"web"),
(7,"View","Not Applicable",0,"web"),
(8,"Add to Cart","Not Applicable",0,"web"),
(9,"Purchase","Not Applicable",0,"web"),
(10,"Offer Page Loaded","Not Applicable",0,"web"),
(11,"Review Submitted","Not Applicable",1,"ios"),
(12,"Review Submitted","Not Applicable",1,"android"),
(13,"Popular Category Tapped","Not Applicable",1,"android"),
(14,"Search the List","Search The List",1,"android"),
(15,"Search the List","Search The List",1,"ios"),
(16,"Searched SP","SP Search - category",0,"web"),
(17,"Searched SP","SP Search - service_provider",0,"web"),
(18,"Searched SP","SP Search - keyword",0,"web"),
(19,"Legacy Ecommerce Purchase","Not Applicable",0,"web"),
(20,"Purchased Cart","Not Applicable",0,"web"),
(21,"Purchased an Item","Not Applicable",0,"web"),
(22,"Purchased","Not Applicable",0,"web"),
(23,"Store Page Loaded","Not Applicable",0,"web"),
(24,"Mobile Activity via Web Request","Not Applicable",0,"web"),
(25,"Web Segment Page Load","Not Applicable",0,"web"),
(26,"Mobile Segment Screen Load","Not Applicable",1,"android"),
(27,"Mobile Segment Screen Load","Not Applicable",1,"ios"),
(28,"BigDeal Feedback Interstitial Page","Not Applicable",0,"web"),
(29,"Blended Request","Not Applicable",0,"web"),
(30,"Category Request","Not Applicable",0,"web"),
(31,"Coupon Request","Not Applicable",0,"web"),
(32,"Keyword Request","Not Applicable",0,"web"),
(33,"KeywordBought Request","Not Applicable",0,"web"),
(34,"MyAngie Request","Not Applicable",0,"web"),
(35,"Quick Quote Request","Not Applicable",0,"web"),
(36,"Service Provider Request","Not Applicable",0,"web"),
(37,"Submitted as Report","Not Applicable",0,"web"),
(-1,"Missing","Missing",-1,"Missing"),
(-2,"Not Applicable","Not Applicable",-2,"Not Applicable"),
(-3,"Unknown","Unknown",-3,"Unknown");