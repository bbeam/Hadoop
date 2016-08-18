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
filter_paid_pay_status_table_legacy_mbr_pay_status = 
	FILTER table_legacy_mbr_pay_status BY pay_status=='Paid';
	
groupby_table_legacy_mbr_pay_status_memberid = 
	GROUP filter_paid_pay_status_table_legacy_mbr_pay_status BY member_id;

gen_groupby_table_legacy_mbr_pay_status_memberid = 
	FOREACH groupby_table_legacy_mbr_pay_status_memberid 
	GENERATE group AS member_id:INT, MAX(filter_paid_pay_status_table_legacy_mbr_pay_status.id) AS MaxPaid:INT;
	
join_paid_paystatus_with_table_legacy = 
	JOIN table_legacy_mbr_pay_status BY member_id, gen_groupby_table_legacy_mbr_pay_status_memberid BY member_id;

gen_join_paid_paystatus_with_table_legacy =
	FOREACH join_paid_paystatus_with_table_legacy 
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
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::exclude_test_member_id AS exclude_test_member_id: int,
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::postal_address_id AS postal_address_id: int,
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::market_zone_id AS market_zone_id: int,
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::postal_code AS postal_code: chararray,
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::pay_status AS pay_status: chararray,
		     gen_lojoin_join_base_membersplus_with_table_legacy_member_membership_tier::membership_tier_name AS membership_tier_name: chararray,
		     table_al4_t_user::first_name AS first_name: chararray, 
		     table_al4_t_user::last_name AS last_name: chararray, 
		     table_al4_t_user::status AS status: chararray, 
		     table_al4_t_user::test_user AS test_user: int;

filter_isprimary_table_al4_t_user_address = 
	FILTER table_al4_t_user_address BY is_primary==1;
	
groupby_table_al4_t_user_address_user_id = 
	GROUP filter_isprimary_table_al4_t_user_address BY user_id;

gen_groupby_table_al4_t_user_address_user_id = 
	FOREACH groupby_table_al4_t_user_address_user_id 
	GENERATE group AS user_id:INT, MAX(filter_isprimary_table_al4_t_user_address.update_date) AS maxupdatedate;

join_t_user_with_gen_groupby_table_al4_t_user_address_user_id = 
	JOIN gen_lojoin_base_membersplus_with_t_user BY user_id, gen_groupby_table_al4_t_user_address_user_id BY user_id;

gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id =
	FOREACH join_t_user_with_gen_groupby_table_al4_t_user_address_user_id 
	GENERATE gen_lojoin_base_membersplus_with_t_user::member_id AS member_id: int,
		     gen_lojoin_base_membersplus_with_t_user::user_id AS user_id: int,
		     gen_lojoin_base_membersplus_with_t_user::exclude_test_member_id AS exclude_test_member_id: int,
		     gen_lojoin_base_membersplus_with_t_user::postal_address_id AS postal_address_id: int,
		     gen_lojoin_base_membersplus_with_t_user::market_zone_id AS market_zone_id: int,
		     gen_lojoin_base_membersplus_with_t_user::postal_code AS postal_code: chararray,
		     gen_lojoin_base_membersplus_with_t_user::pay_status AS pay_status: chararray,
		     gen_lojoin_base_membersplus_with_t_user::membership_tier_name AS membership_tier_name: chararray,
		     gen_lojoin_base_membersplus_with_t_user::first_name AS first_name: chararray, 
		     gen_lojoin_base_membersplus_with_t_user::last_name AS last_name: chararray, 
		     gen_lojoin_base_membersplus_with_t_user::status AS status: chararray, 
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
	
lojoin_base_members_with_postal_address_id = 
	JOIN gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id BY postal_address_id LEFT OUTER,
	     gen_groupby_table_al4_t_user_address_userid_updatedate BY maxpostaladdressid;	
	
filter_lojoin_base_members_with_postal_address_id = 
	FILTER lojoin_base_members_with_postal_address_id BY 
	gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::t_user_user_id == gen_groupby_table_al4_t_user_address_userid_updatedate::user_id and 
	gen_groupby_table_al4_t_user_address_userid_updatedate::update_date == gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::maxupdatedate;

gen_filter_lojoin_base_members_with_postal_address_id = 
	FOREACH filter_lojoin_base_members_with_postal_address_id 
	GENERATE gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::member_id AS member_id: int,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::user_id AS user_id: int,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::exclude_test_member_id AS exclude_test_member_id: int,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::postal_address_id AS postal_address_id: int,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::market_zone_id AS market_zone_id: int,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::postal_code AS postal_code: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::pay_status AS pay_status: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::membership_tier_name AS membership_tier_name: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::first_name AS first_name: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::last_name AS last_name: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::status AS status: chararray,
		     gen_join_base_membersplus_with_gen_groupby_table_al4_t_user_address_user_id::test_user AS test_user: int,
		     gen_groupby_table_al4_t_user_address_userid_updatedate::update_date AS update_date: datetime,
		     gen_groupby_table_al4_t_user_address_userid_updatedate::maxpostaladdressid AS maxpostaladdressid: int;

/* Step 12: Left join with AngiesList.dbo.t_PostalAddress on PostalAddressId to derive AdvertisingZoneID where primary address of the member falls as AdvertisingZoneID_TypeII */
lojoin_gen_filter_lojoin_base_members_with_postal_address_id = 
	JOIN gen_filter_lojoin_base_members_with_postal_address_id BY maxpostaladdressid LEFT OUTER,
		 table_al4_t_postal_address BY postal_address_id;

gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id = 
	FOREACH lojoin_gen_filter_lojoin_base_members_with_postal_address_id 
	GENERATE gen_filter_lojoin_base_members_with_postal_address_id::member_id AS member_id: int,
			 gen_filter_lojoin_base_members_with_postal_address_id::user_id AS user_id: int,
			 gen_filter_lojoin_base_members_with_postal_address_id::exclude_test_member_id AS exclude_test_member_id: int,
			 gen_filter_lojoin_base_members_with_postal_address_id::postal_address_id AS postal_address_id: int,
			 gen_filter_lojoin_base_members_with_postal_address_id::market_zone_id AS market_zone_id: int,
			 gen_filter_lojoin_base_members_with_postal_address_id::postal_code AS postal_code: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::pay_status AS pay_status: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::membership_tier_name AS membership_tier_name: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::first_name AS first_name: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::last_name AS last_name: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::status AS status: chararray,
			 gen_filter_lojoin_base_members_with_postal_address_id::test_user AS test_user: int,
			 table_al4_t_postal_address::advertising_zone AS advertising_zone: int;


/* ++Step 13: Left join the resulting table with AngiesList.dbo.t_ContactInformation on ContactInformationId to derive PrimaryPhoneNumber and Email. Now we have the member's contact details and primary address derived from both legacy and new system. */
filter_isprimary_table_al4_t_contact_information = 
	FILTER table_al4_t_contact_information BY is_primary==1;

groupby_table_al4_t_contact_information_contextentityid = 
	GROUP filter_isprimary_table_al4_t_contact_information BY context_entity_id;

gen_groupby_table_al4_t_contact_information_contextentityid = 
	FOREACH groupby_table_al4_t_contact_information_contextentityid 
	GENERATE group AS user_id:INT, MAX(filter_isprimary_table_al4_t_contact_information.update_date) AS maxupdatedate;

lojoin_base_member_with_gen_groupby_t_contact_information = 
	JOIN gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id BY user_id LEFT OUTER,
	     gen_groupby_table_al4_t_contact_information_contextentityid by user_id;

gen_lojoin_base_member_with_gen_groupby_t_contact_information = 
	FOREACH lojoin_base_member_with_gen_groupby_t_contact_information 
	GENERATE gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::member_id AS member_id: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::user_id AS user_id: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::exclude_test_member_id AS exclude_test_member_id: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::postal_address_id AS postal_address_id: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::market_zone_id AS market_zone_id: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::postal_code AS postal_code: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::pay_status AS pay_status: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::membership_tier_name AS membership_tier_name: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::first_name AS first_name: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::last_name AS last_name: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::status AS status: chararray,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::test_user AS test_user: int,
			gen_base_member_lojoin_gen_filter_lojoin_base_members_with_postal_address_id::advertising_zone AS advertising_zone: int,
			gen_groupby_table_al4_t_contact_information_contextentityid::maxupdatedate AS maxupdatedate;

groupby_table_al4_t_contact_information_contextentityid_updatedate = 
	GROUP 























	
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
