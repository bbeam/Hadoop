/*
PIG SCRIPT    : tf_dim_member.pig
AUTHOR        : Varun Rauthan
DATE          : Tue Aug 16 
DESCRIPTION   : Data Transformation script for dim_member dimension
*/



/* Reading the input tables */
table_legacy_members=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_members'
	USING org.apache.hive.hcatalog.pig.HCatLoader();

table_legacy_member_address=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_member_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_postal_address=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_postal_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();

table_legacy_membership_tier=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_membership_tier'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_member_membership_tier=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_member_membership_tier'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_member_primary_address=
	LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_member_primary_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_exclude_test_member_ids=
	LOAD '$GOLD_LEGACY_REPORTS_DBO_DB.dq_exclude_test_member_ids'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_mbr_pay_status=
	LOAD '$GOLD_LEGACY_REPORTS_DBO_DB.dq_mbr_pay_status'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_user=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_user'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_al4_t_contact_information=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_contact_information'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_postal_address=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_postal_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_associate_permission=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_associate_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_employee_permission=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_employee_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_member_permission=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_member_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_user_address=
	LOAD '$GOLD_ALWEB_AL_DB.dq_t_user_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_dim_market=
        LOAD '$GOLD_SHARED_DIM_DB.dim_market'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Step 1: Angie.dbo.Members left join AngiesList.dbo.t_User on t_User.AlId = Members.MemberID to get all members from legacy and those from legacy that exist in the new system. */
lojoin_members_with_t_User = 
	JOIN table_legacy_members by member_id LEFT OUTER, table_al4_t_user by al_id;

gen_lojoin_members_with_t_User = 
	FOREACH lojoin_members_with_t_User 
	GENERATE member_id AS member_id:INT, user_id AS user_id:INT;
	
distinct_gen_lojoin_members_with_t_User = 
	DISTINCT gen_lojoin_members_with_t_User;

/* Step 2: Find all members from the new system not in the legacy, Inner join AngiesList.dbo.t_User and AngiesList.dbo.t_MemberPermission on UserId Where t_user.AlId IS NULL */
filter_table_al4_t_user_by_null_al_id = 
	FILTER table_al4_t_user by al_id is null;

ijoin_t_user_with_t_member_permission = 
	JOIN filter_table_al4_t_user_by_null_al_id by user_id, table_al4_t_member_permission by user_id;
	
gen_ijoin_t_user_with_t_member_permission = 
	FOREACH ijoin_t_user_with_t_member_permission
	GENERATE filter_table_al4_t_user_by_null_al_id::al_id AS member_id:INT, filter_table_al4_t_user_by_null_al_id::user_id AS user_id:INT;
	
distinct_gen_ijoin_t_user_with_t_member_permission = 
	DISTINCT gen_ijoin_t_user_with_t_member_permission;
	
/* Step 3: Union both the datasets. This forms the base table for further joins consisting of all members from legacy and new system. */
base_members_legacy_with_al4 = UNION distinct_gen_lojoin_members_with_t_User ,distinct_gen_ijoin_t_user_with_t_member_permission;


/* Step 4: Derive member details (first name, last name, homephone, email) and member subscription details (member date, expiration and status) from Angie.dbo.Members */
join_base_members_with_members = 
	JOIN base_members_legacy_with_al4 BY member_id LEFT OUTER, table_legacy_members BY member_id;

gen_member_details_and_subscription_details = FOREACH join_base_members_with_members 
										  GENERATE base_members_legacy_with_al4::member_id AS member_id:INT, base_members_legacy_with_al4::user_id AS user_id: int, market, first_name, last_name, home_phone, email, member_date, expiration, status;


/* Step 5: Left join with Reports.dbo.ExcludeTestMemberIDs on MemberID to derive TestMemberUserFlag. */
lojoin_base_members_with_exclude_test_member_ids = 
	JOIN gen_member_details_and_subscription_details by member_id LEFT OUTER, table_legacy_exclude_test_member_ids by member_id;

gen_lojoin_base_members_with_exclude_test_member_ids = 
	FOREACH lojoin_base_members_with_exclude_test_member_ids
	GENERATE gen_member_details_and_subscription_details::member_id AS member_id: int,
	gen_member_details_and_subscription_details::user_id AS user_id: int,
	market, first_name, last_name, home_phone, email, member_date, expiration, status,
	table_legacy_exclude_test_member_ids::member_id AS exclude_test_member_id: int;
	


/* Step 6: Derive the PostalAddressID and MarketZoneId of member primary address with Angie.dbo.MemberAddress INNER JOIN Angie.dbo.MemberPrimaryAddress on MemberId and MemberAddressId */
ijoin_member_address_with_member_primary_address = 
	JOIN table_legacy_member_address BY (member_id, member_address_id),  table_legacy_member_primary_address BY (member_id, member_address_id);

gen_ijoin_member_address_with_member_primary_address = 
	FOREACH ijoin_member_address_with_member_primary_address
	GENERATE table_legacy_member_address::postal_address_id AS postal_address_id:INT, 
			 table_legacy_member_address::market_zone_id AS market_zone_id:INT,
			 table_legacy_member_primary_address::member_id AS member_id:INT;

lojoin_base_membersplus_with_m_address = 
	JOIN gen_lojoin_base_members_with_exclude_test_member_ids by member_id LEFT OUTER, gen_ijoin_member_address_with_member_primary_address by member_id;
	
gen_lojoin_base_membersplus_with_m_address = 
	FOREACH lojoin_base_membersplus_with_m_address
	GENERATE gen_lojoin_base_members_with_exclude_test_member_ids::member_id AS member_id: int,
			 gen_lojoin_base_members_with_exclude_test_member_ids::user_id AS user_id: int,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::market AS market: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::first_name AS first_name: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::last_name AS last_name: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::home_phone AS home_phone: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::email AS email: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::member_date AS member_date: datetime,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::expiration AS expiration: datetime,
			 gen_lojoin_base_members_with_exclude_test_member_ids::gen_member_details_and_subscription_details::table_legacy_members::status AS status: chararray,
			 gen_lojoin_base_members_with_exclude_test_member_ids::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_ijoin_member_address_with_member_primary_address::postal_address_id AS postal_address_id: int,
			 gen_ijoin_member_address_with_member_primary_address::market_zone_id AS market_zone_id: int;
	

/* Step 7: Left join with it Angie.dbo.PostalAddress on PostalAddressId to derive PostalCode. So now we also have the postal info of the member primary address. */
lojoin_base_membersplus_with_postal_address = 
	JOIN gen_lojoin_base_membersplus_with_m_address BY postal_address_id LEFT OUTER, table_legacy_postal_address BY postal_address_id;
	
gen_lojoin_base_membersplus_with_postal_address = 
	FOREACH lojoin_base_membersplus_with_postal_address
	GENERATE gen_lojoin_base_membersplus_with_m_address::member_id AS member_id: int,
			 gen_lojoin_base_membersplus_with_m_address::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_m_address::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_m_address::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_m_address::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_m_address::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_m_address::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_m_address::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_m_address::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_m_address::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_m_address::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_m_address::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_m_address::market_zone_id AS market_zone_id: int,
			 table_legacy_postal_address::postal_code AS postal_code: chararray;


/* Step 8: Derive PayStatus by applying left join with [Reports].[dbo].[MbrPayStatus] on MemberID to select distinct Member and paystatus where paystatus is based on maximum ID if paystatus = 'Paid' for a given Member ID */
filter_paid_pay_status_table_legacy_mbr_pay_status = 
	FILTER table_legacy_mbr_pay_status BY pay_status=='Paid';
	
groupby_table_legacy_mbr_pay_status_memberid = 
	GROUP filter_paid_pay_status_table_legacy_mbr_pay_status BY member_id;

gen_groupby_table_legacy_mbr_pay_status_memberid = 
	FOREACH groupby_table_legacy_mbr_pay_status_memberid 
	GENERATE group AS member_id:INT, MAX(filter_paid_pay_status_table_legacy_mbr_pay_status.id) AS MaxPaid:INT;
	
lojoin_paid_paystatus_with_table_legacy = 
	JOIN table_legacy_mbr_pay_status BY member_id LEFT OUTER, gen_groupby_table_legacy_mbr_pay_status_memberid BY member_id;

gen_join_paid_paystatus_with_table_legacy =
	FOREACH lojoin_paid_paystatus_with_table_legacy 
	GENERATE table_legacy_mbr_pay_status::member_id AS member_id: int,
			 table_legacy_mbr_pay_status::pay_status AS pay_status: chararray,
			 table_legacy_mbr_pay_status::id AS id:INT,
			 (gen_groupby_table_legacy_mbr_pay_status_memberid::MaxPaid is null?table_legacy_mbr_pay_status::id:gen_groupby_table_legacy_mbr_pay_status_memberid::MaxPaid) AS derived_id:INT;

filt_gen_join_paid_paystatus_with_table_legacy = 
	FILTER gen_join_paid_paystatus_with_table_legacy BY id==derived_id;
	
distinct_table_legacy_mbr_pay_status = 
	DISTINCT (FOREACH filt_gen_join_paid_paystatus_with_table_legacy 
			  GENERATE member_id, pay_status);

lojoin_base_membersplus_with_mbr_pay_status = 
	JOIN gen_lojoin_base_membersplus_with_postal_address BY member_id LEFT OUTER,
		 distinct_table_legacy_mbr_pay_status BY member_id;

gen_lojoin_base_membersplus_with_mbr_pay_status = 
	FOREACH lojoin_base_membersplus_with_mbr_pay_status
	GENERATE gen_lojoin_base_membersplus_with_postal_address::member_id AS member_id: int,
			 gen_lojoin_base_membersplus_with_postal_address::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_postal_address::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_postal_address::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_postal_address::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_postal_address::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_postal_address::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_postal_address::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_membersplus_with_postal_address::postal_code AS postal_code: chararray,
			 distinct_table_legacy_mbr_pay_status::pay_status AS pay_status: chararray;


/* Step 9: MembershipTierName_TypeII is Angie.dbo.MembershipTier.MembershipTierName which is joined with Angie.dbo.MemberMembershipTier on MembershipTierID which has MemberID as foreign key.*/
join_table_legacy_membership_tier_with_table_legacy_member_membership_tier = 
	JOIN table_legacy_membership_tier BY membership_tier_id, table_legacy_member_membership_tier BY membership_tier_id;

lojoin_join_base_membersplus_with_table_legacy_member_membership_tier = 
	JOIN gen_lojoin_base_membersplus_with_mbr_pay_status BY member_id LEFT OUTER, join_table_legacy_membership_tier_with_table_legacy_member_membership_tier BY member_id;
	
	
gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier = 
	FOREACH lojoin_join_base_membersplus_with_table_legacy_member_membership_tier
	GENERATE gen_lojoin_base_membersplus_with_mbr_pay_status::member_id AS member_id: int,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::postal_code AS postal_code: chararray,
			 gen_lojoin_base_membersplus_with_mbr_pay_status::pay_status AS pay_status: chararray,
			 join_table_legacy_membership_tier_with_table_legacy_member_membership_tier::table_legacy_membership_tier::membership_tier_name AS membership_tier_name: chararray;


/* Step 10: Find the current primary address of each user in the new system. To do this, select all primary addresses (IsPrimary = 1) from AngiesList.dbo.t_UserAddress and find the latest record for each user on UpdateDate. Left join on UserId. */
lojoin_base_membersplus_with_t_user = 
	JOIN gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier BY user_id LEFT OUTER,
		 table_al4_t_user BY user_id;
	 
gen_lojoin_base_membersplus_with_t_user = 
	FOREACH lojoin_base_membersplus_with_t_user 
	GENERATE gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::member_id AS member_id: int,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::user_id AS user_id: int,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::market AS market: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::first_name AS first_name: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::last_name AS last_name: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::home_phone AS home_phone: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::email AS email: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::member_date AS member_date: datetime,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::expiration AS expiration: datetime,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::status AS status: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::postal_address_id AS postal_address_id: int,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::market_zone_id AS market_zone_id: int,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::postal_code AS postal_code: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::pay_status AS pay_status: chararray,
			 gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::membership_tier_name AS membership_tier_name: chararray,
			 table_al4_t_user::user_id AS tu_user_id: int,
			 table_al4_t_user::status AS tu_status: chararray,
			 table_al4_t_user::first_name AS tu_first_name: chararray,
			 table_al4_t_user::last_name AS tu_last_name: chararray,
		     table_al4_t_user::test_user AS test_user: int;

filter_isprimary_table_al4_t_user_address = 
	FILTER table_al4_t_user_address BY is_primary==1;
	
groupby_table_al4_t_user_address_user_id = 
	GROUP filter_isprimary_table_al4_t_user_address BY user_id;

gen_groupby_table_al4_t_user_address_user_id = 
	FOREACH groupby_table_al4_t_user_address_user_id 
	GENERATE group AS user_id:INT, MAX(filter_isprimary_table_al4_t_user_address.update_date) AS maxupdatedate;

lojoin_t_user_with_gen_groupby_table_al4_t_user_address_user_id = 
	JOIN gen_lojoin_base_membersplus_with_t_user BY user_id LEFT OUTER, gen_groupby_table_al4_t_user_address_user_id BY user_id;

gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id =
	FOREACH lojoin_t_user_with_gen_groupby_table_al4_t_user_address_user_id 
	GENERATE gen_lojoin_base_membersplus_with_t_user::member_id AS member_id: int,
		     gen_lojoin_base_membersplus_with_t_user::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_t_user::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_t_user::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_t_user::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_t_user::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_t_user::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_t_user::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_t_user::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_t_user::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_t_user::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_t_user::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_t_user::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_membersplus_with_t_user::postal_code AS postal_code: chararray,
			 gen_lojoin_base_membersplus_with_t_user::pay_status AS pay_status: chararray,
			 gen_lojoin_base_membersplus_with_t_user::membership_tier_name AS membership_tier_name: chararray,
			 gen_lojoin_base_membersplus_with_t_user::tu_user_id AS tu_user_id: int,
			 gen_lojoin_base_membersplus_with_t_user::tu_status AS tu_status: chararray,
			 gen_lojoin_base_membersplus_with_t_user::tu_first_name AS tu_first_name: chararray,
			 gen_lojoin_base_membersplus_with_t_user::tu_last_name AS tu_last_name: chararray,
		     gen_lojoin_base_membersplus_with_t_user::test_user AS test_user: int,
			 gen_groupby_table_al4_t_user_address_user_id::user_id AS t_user_user_id: int,
			 gen_groupby_table_al4_t_user_address_user_id::maxupdatedate AS maxupdatedate;


/* Step 11: To accomodate cases of more than one address change in a single day, filter all primary addresses (IsPrimary = 1), group by UserId and UpdateDate to find max of PostalAddressId. Left Join on UserId and UpdateDate.*/
groupby_table_al4_t_user_address_userid_updatedate = 
	GROUP filter_isprimary_table_al4_t_user_address BY (user_id,update_date);

gen_groupby_table_al4_t_user_address_userid_updatedate = 
	FOREACH groupby_table_al4_t_user_address_userid_updatedate 
	GENERATE group.user_id AS user_id:INT, group.update_date AS update_date:DATETIME, 
	         MAX(filter_isprimary_table_al4_t_user_address.postal_address_id) AS maxpostaladdressid:INT;
	
lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate = 
	JOIN gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id BY (t_user_user_id, maxupdatedate) LEFT OUTER,
		 gen_groupby_table_al4_t_user_address_userid_updatedate BY (user_id, update_date);
	
gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate = 
	FOREACH lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate 
	GENERATE gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::member_id AS member_id: int,
		     gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::postal_code AS postal_code: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::pay_status AS pay_status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::membership_tier_name AS membership_tier_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::tu_user_id AS tu_user_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::tu_status AS tu_status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::tu_first_name AS tu_first_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::tu_last_name AS tu_last_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::test_user AS test_user: int,
			 gen_groupby_table_al4_t_user_address_userid_updatedate::update_date AS update_date: datetime,
			 gen_groupby_table_al4_t_user_address_userid_updatedate::maxpostaladdressid AS maxpostaladdressid: int;


/* Step 12: Left join with AngiesList.dbo.t_PostalAddress on PostalAddressId to derive AdvertisingZoneID where primary address of the member falls as AdvertisingZoneID_TypeII */
lojoin_gen_filter_lojoin_base_members_with_postal_address_id = 
	JOIN gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate BY maxpostaladdressid LEFT OUTER,
		 table_al4_t_postal_address BY postal_address_id;

gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id = 
	FOREACH lojoin_gen_filter_lojoin_base_members_with_postal_address_id 
	GENERATE gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::member_id AS member_id: int,
		     gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::user_id AS user_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::market AS market: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::first_name AS first_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::last_name AS last_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::home_phone AS home_phone: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::email AS email: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::member_date AS member_date: datetime,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::expiration AS expiration: datetime,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::status AS status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::postal_code AS postal_code: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::pay_status AS pay_status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::membership_tier_name AS membership_tier_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::tu_user_id AS tu_user_id: int,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::tu_status AS tu_status: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::tu_first_name AS tu_first_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::tu_last_name AS tu_last_name: chararray,
			 gen_lojoin_base_membersplus_with_gen_groupby_table_al4_t_user_address_userid_updatedate::test_user AS test_user: int,
			 table_al4_t_postal_address::advertising_zone AS advertising_zone: int;


/* ++Step 13: Left join the resulting table with AngiesList.dbo.t_ContactInformation on ContactInformationId to derive PrimaryPhoneNumber and Email. Now we have the member's contact details and primary address derived from both legacy and new system. */
filter_isprimary_table_al4_t_contact_information = 
	FILTER table_al4_t_contact_information BY is_primary==1 AND entity_type=='user';

groupby_table_al4_t_contact_information_contextentityid = 
	GROUP filter_isprimary_table_al4_t_contact_information BY context_entity_id;

gen_groupby_table_al4_t_contact_information_contextentityid = 
	FOREACH groupby_table_al4_t_contact_information_contextentityid 
	GENERATE group AS context_entity_id:INT, MAX(filter_isprimary_table_al4_t_contact_information.update_date) AS maxupdatedate;

lojoin_base_member_with_gen_groupby_t_contact_information = 
	JOIN gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id BY user_id LEFT OUTER,
	     gen_groupby_table_al4_t_contact_information_contextentityid by context_entity_id;

gen_lojoin_base_member_with_gen_groupby_t_contact_information = 
	FOREACH lojoin_base_member_with_gen_groupby_t_contact_information 
	GENERATE gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::member_id AS member_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::user_id AS user_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::market AS market: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::first_name AS first_name: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::last_name AS last_name: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::home_phone AS home_phone: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::email AS email: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::member_date AS member_date: datetime,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::expiration AS expiration: datetime,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::status AS status: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::postal_address_id AS postal_address_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::market_zone_id AS market_zone_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::postal_code AS postal_code: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::pay_status AS pay_status: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::membership_tier_name AS membership_tier_name: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::tu_user_id AS tu_user_id: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::tu_status AS tu_status: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::tu_first_name AS tu_first_name: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::tu_last_name AS tu_last_name: chararray,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::test_user AS test_user: int,
			 gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::advertising_zone AS advertising_zone: int,
			 gen_groupby_table_al4_t_contact_information_contextentityid::context_entity_id AS context_entity_id:INT,
			 gen_groupby_table_al4_t_contact_information_contextentityid::maxupdatedate AS maxupdatedate;

groupby_table_al4_t_contact_information_contextentityid_updatedate = 
	GROUP filter_isprimary_table_al4_t_contact_information by (context_entity_id,update_date);

gen_groupby_table_al4_t_contact_information_contextentityid_updatedate = 
	FOREACH groupby_table_al4_t_contact_information_contextentityid_updatedate 
	GENERATE group.context_entity_id AS context_entity_id:INT, group.update_date AS update_date:DATETIME, 
	         MAX(filter_isprimary_table_al4_t_contact_information.contact_information_id) AS maxcontactinformationid:INT;

lojoin_base_members_with_t_contact_information = 
	JOIN gen_lojoin_base_member_with_gen_groupby_t_contact_information BY (context_entity_id, maxupdatedate) LEFT OUTER,
	     gen_groupby_table_al4_t_contact_information_contextentityid_updatedate BY (context_entity_id, update_date);
	     
lojoin_base_members_with_table_al4_t_contact_information = 
	JOIN lojoin_base_members_with_t_contact_information BY gen_groupby_table_al4_t_contact_information_contextentityid_updatedate::maxcontactinformationid LEFT OUTER,
		 table_al4_t_contact_information BY contact_information_id;

gen_lojoin_base_members_with_table_al4_t_contact_information = 
	FOREACH lojoin_base_members_with_table_al4_t_contact_information 
	GENERATE lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::member_id AS member_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::user_id AS user_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::market AS market: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::first_name AS first_name: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::last_name AS last_name: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::home_phone AS home_phone: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::email AS email: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::member_date AS member_date: datetime,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::expiration AS expiration: datetime,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::status AS status: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::exclude_test_member_id AS exclude_test_member_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::postal_address_id AS postal_address_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::market_zone_id AS market_zone_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::postal_code AS postal_code: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::pay_status AS pay_status: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::membership_tier_name AS membership_tier_name: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::tu_user_id AS tu_user_id: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::tu_status AS tu_status: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::tu_first_name AS tu_first_name: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::tu_last_name AS tu_last_name: chararray,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::test_user AS test_user: int,
			 lojoin_base_members_with_t_contact_information::gen_lojoin_base_member_with_gen_groupby_t_contact_information::advertising_zone AS advertising_zone: int,
			 table_al4_t_contact_information::primary_phonenumber AS primary_phonenumber: chararray, 
			 table_al4_t_contact_information::email AS tci_email: chararray;


/* Step 14: Foreach UserId in AngiesList.dbo.t_AssociatePermission, find avg(AssociatePermissionId) and left join it with the base table on UserId. This will be used to set the AssociateFlag. */
groupby_table_al4_t_associate_permission = 
	GROUP table_al4_t_associate_permission BY user_id;
	
gen_groupby_table_al4_t_associate_permission = 
	FOREACH groupby_table_al4_t_associate_permission 
	GENERATE group AS user_id:INT, 1 AS associate:INT;

lojoin_base_members_with_table_al4_t_associate_permission = 
	JOIN gen_lojoin_base_members_with_table_al4_t_contact_information BY user_id LEFT OUTER, gen_groupby_table_al4_t_associate_permission BY user_id;

gen_lojoin_base_members_with_table_al4_t_associate_permission = 
	FOREACH lojoin_base_members_with_table_al4_t_associate_permission 
	GENERATE gen_lojoin_base_members_with_table_al4_t_contact_information::member_id AS member_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::user_id AS user_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::market AS market: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::first_name AS first_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::last_name AS last_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::home_phone AS home_phone: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::email AS email: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::member_date AS member_date: datetime,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::expiration AS expiration: datetime,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::status AS status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::postal_code AS postal_code: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::pay_status AS pay_status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::membership_tier_name AS membership_tier_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::tu_user_id AS tu_user_id: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::tu_status AS tu_status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::tu_first_name AS tu_first_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::tu_last_name AS tu_last_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::test_user AS test_user: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::advertising_zone AS advertising_zone: int,
			 gen_lojoin_base_members_with_table_al4_t_contact_information::primary_phonenumber AS primary_phonenumber: chararray, 
			 gen_lojoin_base_members_with_table_al4_t_contact_information::tci_email AS tci_email: chararray,
			 (gen_groupby_table_al4_t_associate_permission::associate IS NOT NULL ? gen_groupby_table_al4_t_associate_permission::associate : 0) AS associate: int;


/* Step 15: Foreach UserId in AngiesList.dbo.t_EmployeePermission, find avg(EmployeePermissionId) and left join it with the base table on UserId. This will be used to set the EmployeeFlag. */
groupby_table_al4_t_employee_permission = 
	GROUP table_al4_t_employee_permission BY user_id;
	
gen_groupby_table_al4_t_employee_permission = 
	FOREACH groupby_table_al4_t_employee_permission 
	GENERATE group AS user_id:INT, 1 AS employee:INT;

lojoin_base_members_with_table_al4_t_employee_permission = 
	JOIN gen_lojoin_base_members_with_table_al4_t_associate_permission BY user_id LEFT OUTER, gen_groupby_table_al4_t_employee_permission BY user_id;

gen_lojoin_base_members_with_table_al4_t_employee_permission = 
	FOREACH lojoin_base_members_with_table_al4_t_employee_permission 
	GENERATE gen_lojoin_base_members_with_table_al4_t_associate_permission::member_id AS member_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::user_id AS user_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::market AS market: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::first_name AS first_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::last_name AS last_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::home_phone AS home_phone: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::email AS email: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::member_date AS member_date: datetime,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::expiration AS expiration: datetime,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::status AS status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::postal_address_id AS postal_address_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::market_zone_id AS market_zone_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::postal_code AS postal_code: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::pay_status AS pay_status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::membership_tier_name AS membership_tier_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::tu_user_id AS tu_user_id: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::tu_status AS tu_status: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::tu_first_name AS tu_first_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::tu_last_name AS tu_last_name: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::test_user AS test_user: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::advertising_zone AS advertising_zone: int,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::primary_phonenumber AS primary_phonenumber: chararray, 
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::tci_email AS tci_email: chararray,
			 gen_lojoin_base_members_with_table_al4_t_associate_permission::associate AS associate: int,
			 (gen_groupby_table_al4_t_employee_permission::employee IS NOT NULL ? gen_groupby_table_al4_t_employee_permission::employee : 0) AS employee: int;


/* Step 16: Left join Angie.dbo.Members with shared.dim_market on Market and derive Market Key by selecting latest record. If Market is missing, populate Market_Key from dim_Market where market_id = -1 */
lojoin_base_members_with_table_dim_market = 
	JOIN gen_lojoin_base_members_with_table_al4_t_employee_permission BY UPPER(market) LEFT OUTER, table_dim_market BY UPPER(market_nm);

gen_lojoin_base_members_with_table_dim_market = 
	FOREACH lojoin_base_members_with_table_dim_market
	GENERATE gen_lojoin_base_members_with_table_al4_t_employee_permission::member_id AS member_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::user_id AS user_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::market AS market: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::first_name AS first_name: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::last_name AS last_name: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::home_phone AS home_phone: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::email AS email: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::member_date AS member_date: datetime,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::expiration AS expiration: datetime,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::status AS status: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::exclude_test_member_id AS exclude_test_member_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::postal_address_id AS postal_address_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::market_zone_id AS market_zone_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::postal_code AS postal_code: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::pay_status AS pay_status: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::membership_tier_name AS membership_tier_name: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::tu_user_id AS tu_user_id: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::tu_status AS tu_status: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::tu_first_name AS tu_first_name: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::tu_last_name AS tu_last_name: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::test_user AS test_user: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::advertising_zone AS advertising_zone: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::primary_phonenumber AS primary_phonenumber: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::tci_email AS tci_email: chararray,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::associate AS associate: int,
			gen_lojoin_base_members_with_table_al4_t_employee_permission::employee AS employee: int,
			table_dim_market::market_key AS market_key: long,
			table_dim_market::market_nm AS market_nm: chararray;

/* Generating the required schema of dim_member */
gen_dim_member = 
	FOREACH gen_lojoin_base_members_with_table_dim_market
	GENERATE (member_id is null?$NUMERIC_MISSING_KEY:member_id) AS member_id:INT, 
			 (tu_user_id is null?$NUMERIC_MISSING_KEY:tu_user_id) AS user_id: INT, 
			 (tci_email is null?email:tci_email) AS email: chararray, 
			 postal_code AS postal_code:CHARARRAY, 
			 pay_status AS pay_status:CHARARRAY, 
			 (tu_status is null?status:tu_status) AS member_status: chararray, 
			 (member_id is null?null:(CurrentTime() > expiration?'EXPIRED':'NONEXPIRED')) AS expiration_status: chararray, 
			 member_date AS member_dt:datetime, 
			 membership_tier_name AS membership_tier_nm: chararray, 
			 (primary_phonenumber is null?home_phone:primary_phonenumber) AS primary_phone_number: chararray,
			 (tu_first_name is null?first_name:tu_first_name) AS first_nm: chararray, 
			 (tu_last_name is null?last_name:tu_last_name) AS last_nm: chararray, 
			 associate AS associate:INT,
			 employee AS employee:INT,
			 (market_key is null?$NUMERIC_MISSING_KEY:market_key) AS market_key: long,
			 ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') as est_load_timestamp,
			 ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') as utc_load_timestamp;


rmf /user/hadoop/data/work/shareddim/tf_dim_member

STORE gen_dim_member
                INTO '/user/hadoop/data/work/shareddim/tf_dim_member'
                USING PigStorage('\u0001');
 
