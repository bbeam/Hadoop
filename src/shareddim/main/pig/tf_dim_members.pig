/*
PIG SCRIPT    : tf_dim_members.pig
AUTHOR        : Varun Rauthan
DATE          : Tue Aug 16 
DESCRIPTION   : Data Transformation script for dim_members dimension
*/



/* Reading the input tables */
table_legacy_members=
	LOAD 'gold_legacy_angie_dbo.dq_members'
	USING org.apache.hive.hcatalog.pig.HCatLoader();

table_legacy_member_address=
	LOAD 'gold_legacy_angie_dbo.dq_member_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_postal_address=
	LOAD 'gold_legacy_angie_dbo.dq_postal_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();

table_legacy_membership_tier=
	LOAD 'gold_legacy_angie_dbo.dq_membership_tier'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_member_membership_tier=
	LOAD 'gold_legacy_angie_dbo.dq_member_membership_tier'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_member_primary_address=
	LOAD 'gold_legacy_angie_dbo.dq_member_primary_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_exclude_test_member_ids=
	LOAD 'gold_legacy_reports_dbo.dq_exclude_test_member_ids'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_legacy_mbr_pay_status=
	LOAD 'gold_legacy_reports_dbo.dq_mbr_pay_status'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_user=
	LOAD 'gold_alweb_al.dq_t_user'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
	
table_al4_t_contact_information=
	LOAD 'gold_alweb_al.dq_t_contact_information'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_postal_address=
	LOAD 'gold_alweb_al.dq_t_postal_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_associate_permission=
	LOAD 'gold_alweb_al.dq_t_associate_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_employee_permission=
	LOAD 'gold_alweb_al.dq_t_employee_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_member_permission=
	LOAD 'gold_alweb_al.dq_t_member_permission'
	USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_al4_t_user_address=
	LOAD 'gold_alweb_al.dq_t_user_address'
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Step 1: Angie.dbo.Members left join AngiesList.dbo.t_User on t_User.AlId = Members.MemberID to get all members from legacy and those from legacy that exist in the new system. */
lojoin_members_with_t_User = 
	JOIN table_legacy_members by member_id LEFT OUTER, table_al4_t_user by al_id;

gen_lojoin_members_with_t_User = 
	FOREACH lojoin_members_with_t_User 
	GENERATE member_id AS member_id:INT, user_id AS user_id:INT;

/* Step 2: Find all members from the new system not in the legacy, Inner join AngiesList.dbo.t_User and AngiesList.dbo.t_MemberPermission on UserId Where t_user.AlId IS NULL */
filter_table_al4_t_user_by_null_al_id = 
	FILTER table_al4_t_user by al_id is null;

ijoin_t_user_with_t_member_permission = 
	JOIN filter_table_al4_t_user_by_null_al_id by user_id, table_al4_t_member_permission by user_id;
	
gen_ijoin_t_user_with_t_member_permission = 
	FOREACH ijoin_t_user_with_t_member_permission
	GENERATE filter_table_al4_t_user_by_null_al_id::al_id AS member_id:INT, filter_table_al4_t_user_by_null_al_id::user_id AS user_id:INT;
	
	
/* Step 3: Union both the datasets. This forms the base table for further joins consisting of all members from legacy and new system. */
base_members_legacy_with_al4 = UNION gen_lojoin_members_with_t_User ,gen_ijoin_t_user_with_t_member_permission;


/* Step 4: Derive member details (first name, last name, homephone, email) and member subscription details (member date, expiration and status) from Angie.dbo.Members */
member_details_and_subscription_details = FOREACH table_legacy_members 
										  GENERATE first_name, last_name, home_phone, email, member_date, expiration, status;


/* Step 5: Left join with Reports.dbo.ExcludeTestMemberIDs on MemberID to derive TestMemberUserFlag. */
lojoin_base_members_with_exclude_test_member_ids = 
	JOIN base_members_legacy_with_al4 by member_id LEFT OUTER, table_legacy_exclude_test_member_ids by member_id;

gen_lojoin_base_members_with_exclude_test_member_ids = 
	FOREACH lojoin_base_members_with_exclude_test_member_ids
	GENERATE base_members_legacy_with_al4::member_id AS member_id: int,
	base_members_legacy_with_al4::user_id AS user_id: int,
	table_legacy_exclude_test_member_ids::member_id AS exclude_test_member_id: int;
	
/*  TO CHECK
	table_legacy_exclude_test_member_ids::date_added AS date_added: datetime,
	table_legacy_exclude_test_member_ids::est_load_timestamp AS est_load_timestamp: datetime,
	table_legacy_exclude_test_member_ids::utc_load_timestamp AS utc_load_timestamp: datetime;
*/	


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
	gen_lojoin_base_membersplus_with_m_address::exclude_test_member_id AS exclude_test_member_id: int,
	gen_lojoin_base_membersplus_with_m_address::postal_address_id AS postal_address_id: int,
	gen_lojoin_base_membersplus_with_m_address::market_zone_id AS market_zone_id: int,
	table_legacy_postal_address::postal_code AS postal_code: chararray;
	

/* Step 8: Derive PayStatus by applying left join with [Reports].[dbo].[MbrPayStatus] on MemberID to select distinct Member and paystatus where paystatus is based on maximum ID if paystatus = 'Paid' for a given Member ID */
groupby_table_legacy_mbr_pay_status_memberid_paystatus = 
	GROUP table_legacy_mbr_pay_status BY (member_id,pay_status);
	
gen_groupby_table_legacy_mbr_pay_status_memberid_paystatus = 
	FOREACH groupby_table_legacy_mbr_pay_status_memberid_paystatus 
	GENERATE group.member_id AS member_id:INT, group.pay_status AS pay_status:chararray;
	
filter_paid_pay_status_table_legacy_mbr_pay_status = 
	FILTER table_legacy_mbr_pay_status BY pay_status=='Paid';
	
groupby_table_legacy_mbr_pay_status_memberid = 
	GROUP filter_paid_pay_status_table_legacy_mbr_pay_status BY member_id;

gen_groupby_table_legacy_mbr_pay_status_memberid = 
	FOREACH groupby_table_legacy_mbr_pay_status_memberid 
	GENERATE group AS member_id:INT, MAX(table_legacy_mbr_pay_status.id) AS MaxPaid:INT;

join_table_legacy_mbr_pay_status = 
	JOIN 













	
gen_groupby_table_legacy_mbr_pay_status = 
	FOREACH groupby_table_legacy_mbr_pay_status 
	GENERATE group AS member_id:INT, group.pay_status AS pay_status:INT, MAX(table_legacy_mbr_pay_status.id) AS ;

lojoin_base_membersplus_with_mbr_pay_status = 
	JOIN gen_lojoin_base_membersplus_with_postal_address BY member_id LEFT OUTER, gen_groupby_table_legacy_mbr_pay_status BY member_id;













/* Mapping source columns to target */
table_dim_market =
	FOREACH table_legacy_market
	GENERATE market_id  AS market_id:INT, market AS market_nm:CHARARRAY;


/* Loading to target */
STORE table_dim_market into 'work_shared_dim.tf_dim_market'
		using org.apache.hive.hcatalog.pig.HCatStorer();
