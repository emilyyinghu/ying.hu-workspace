call api.update_table_sql('hk', 'masterdata_hk', 'address');


UPDATE masterdata_hk.address 
SET address_dwid = md5(country_code||'__'||'address'||'__'||id) 
WHERE address_dwid isnull ;



select api.get_normalized_name('MT''LA VIE')


------------------------------------------------------------------
------ Refactor hk_warehouse to new `masterdata` structure ------
------------------------------------------------------------------

-- 1.replicate necessary table from redshift 

DROP TABLE masterdata_hk.dm_project;

CREATE TABLE masterdata_hk.dm_project (
	dw_project_id text NULL,
	project_name text NULL,
	project_alias text NULL,
	property_type_id int8 NULL,
	real_estate_type text NULL,
	dw_address_id text NULL,
	address_display_text text NULL,
	ownership_type text NULL,
	tenure text NULL,
	tenure_type text NULL,
	tenure_expiry_text text NULL,
	tenure_start_date text NULL,
	tenure_end_date text NULL,
	project_status text NULL,
	construction_status text NULL,
	construction_start_year text NULL,
	occupancy_date text NULL,
	legal_completion_date text NULL,
	completion_year float4 NULL,
	decommission_year float4 NULL,
	region_state_province text NULL,
	location_marker text NULL,
	latitude float4 NULL,
	longitude float4 NULL,
	facilities_text text NULL,
	special_attributes text NULL,
	is_strata_titled_issued text NULL,
	land_size_sqm float4 NULL,
	project_cost_class text NULL,
	maintenance_fee_basis text NULL,
	maintenance_fee_unit_price float4 NULL,
	project_description text NULL,
	tag_text text NULL,
	parking_text text NULL,
	restrictions_text text NULL,
	project_url text NULL,
	slug text NULL,
	data_source text NULL,
	data_source_key text NULL,
	data_source_additional_id text NULL,
	data_source_count int8 NULL,
	is_active boolean NULL,
	country_code text NULL,
	country text NULL,
	developer_name text NULL
);



DROP TABLE masterdata_hk.dm_property__temp;

CREATE TABLE masterdata_hk.dm_property (
	dw_property_id text NULL,
	dw_building_id text NULL,
	dw_address_id text NULL,
	dw_project_id text NULL,
	project_name text NULL,
	building_name text NULL,
	block_num text NULL,
	address_unit text NULL,
	address_floor text NULL,
	address_floor_num float4 NULL,
	floor_seq float4 NULL,
	address_stack text NULL,
	address_floor_level text NULL,
	address_street text NULL,
	address_num text NULL,
	dw_land_id text NULL,
	postal_code text NULL,
	source_building_key text NULL,
	latitude float4 NULL,
	longitude float4 NULL,
	slug text NULL,
	gross_area_inferred_sqm float4 NULL,
	gross_area_inferred_sqft float4 NULL,
	net_area_inferred_sqm float4 NULL,
	net_area_inferred_sqft float4 NULL,
	net_floor_area_sqm float4 NULL,
	net_floor_area_sqft float4 NULL,
	gross_floor_area_sqm float4 NULL,
	gross_floor_area_sqft float4 NULL,
	ownership_type text NULL,
	property_type_id int8 NULL,
	dw_unit_group_id text NULL,
	construction_start text NULL,
	construction_end text NULL,
	completion_year text NULL,
	location_marker text NULL,
	land_use text NULL,
	gross_plot_ratio float4 NULL,
	lot_size_sqm float4 NULL,
	property_facilities_text text NULL,
	display_text text NULL,
	occupancy_date text NULL,
	data_source_key text NULL,
	bedroom_count float4 NULL,
	bathroom_count float4 NULL,
	other_room_count float4 NULL,
	floors_below_ground text NULL,
	floors_above_ground text NULL,
	min_floor_num text NULL,
	max_floor_num text NULL,
	min_stack text NULL,
	max_stack text NULL,
	is_top_floor text NULL,
	is_bottom_floor text NULL,
	is_rental_only text NULL,
	is_penthouse text NULL,
	is_active boolean NULL,
	is_address_active boolean NULL,
	country_code text NULL,
	country text NULL,
	city text NULL,
	city_area text NULL,
	city_subarea text NULL,
	data_source text NULL,
	data_source_count int8 NULL,
	data_source_type text NULL
);


-- 1.address

select *
from masterdata_hk.dm_address da 
limit 500


select *
from masterdata_sg.address da 
limit 500


select development , development_phase , address_building , array_agg(address_number) as address_number_list
from masterdata_hk.dm_address da
where da.address_type = 'point' --da.address_type = 'locality' and da.address_type_attribute in ('project', 'street', 'building')
group by 1,2,3



--insert into masterdata_hk.dm_address
-- 'lower() as '

insert into masterdata_hk.address
(
	address_type_code,address_type_attribute,full_address_text,address_display_text,address_building,address_number,
	address_number_range,address_number_range_start,address_number_range_end,address_street_text,street_type,
	street_prefix,street_prefix_type,street_side,street_name,street_name_root,street_suffix,postal_code_ext,postal_code,
	development,development_code,development_phase,development_phase_number,neighborhood_block,neighborhood_block_code,
	neighborhood_section,neighborhood,neighborhood_code,neighborhood_group,city_subarea,city_subarea_id, city_subarea_code,city_district,
	city_area,city_area_id,city_area_code,city,city_id,city_code,metro_area,metro_area_code,metro_area_district,region_admin_subdistrict,
	region_admin_subdistrict_code,region_admin_district,region_admin_district_id,region_admin_district_code,region_state_province,
	region_state_province_code,region_state_province_alias,territory,territory_code,country,country_code,country_3code,geographic_zone,
	continent,latitude,longitude,location_marker,slug,status_code,language_code,data_source_count,hash_key,dw_address_id
)
select
	--id,
	--address_dwid,
	case when da.address_type = 'point' then 'point-address'
		when da.address_type = 'locality' and da.address_type_attribute = 'project' then 'project-address'
		when da.address_type = 'locality' and da.address_type_attribute = 'building' then 'building-address'
		when da.address_type = 'locality' and da.address_type_attribute = 'street' then 'street-address'
		end as address_type_code,
	null as address_type_attribute,
	lower(da.full_address_text) as full_address_text,
	initcap(da.full_address_text) as address_display_text,
	lower(da.address_building) as address_building,
	da.address_number as address_number,
	da.address_number_range as address_number_range,
	da.address_number_range_start as address_number_range_start,
	da.address_number_range_end as address_number_range_end,
	lower(da.address_street_text) as address_street_text,
	lower(da.street_type) as street_type,
	lower(da.street_prefix) as street_prefix,
	lower(da.street_prefix_type) as street_prefix_type,
	lower(da.street_side) as street_side,
	lower(da.street_name) as street_name,
	lower(da.street_name) as street_name_root,
	lower(da.street_suffix) as street_suffix,
	null as postal_code_ext, -- HK does not have postalcode
	null as postal_code, -- HK does not have postalcode
	lower(da.development) as development,
	lower(da.development_code) as development_code,
	lower(da.development_phase) as development_phase,
	null as development_phase_number,
	null as neighborhood_block,
	null as neighborhood_block_code,
	null as neighborhood_section,
	lower(da.neighborhood) as neighborhood,
	lower(da.neighborhood_code) as neighborhood_code,
	lower(da.neighborhood_group) as neighborhood_group,
	lower(da.city_subarea) as city_subarea,
	lower(ah.city_subarea_id) as city_subarea_id,
	lower(da.city_subarea_code) as city_subarea_code,
	lower(da.city_district) as city_district,
	lower(da.city_area) as city_area,
	lower(ah.city_area_id) as city_area_id,
	lower(da.city_area_code) as city_area_code,
	lower(da.city) as city,
	lower(ah.city_id) as city_id,
	lower(da.city_code) as city_code,
	lower(da.metro_area) as metro_area,
	lower(da.metro_area_code) as metro_area_code,
	lower(da.metro_area_district) as metro_area_district,
	lower(da.region_admin_subdistrict) as region_admin_subdistrict,
	lower(da.region_admin_subdistrict_code) as region_admin_subdistrict_code,
	lower(da.region_admin_district) as region_admin_district,
	null as region_admin_district_id,
	lower(da.region_admin_district_code) as region_admin_district_code,
	lower(da.region_state_province) as region_state_province,
	lower(da.region_state_province_code) as region_state_province_code,
	lower(da.region_state_province_alias) as region_state_province_alias,
	lower(da.territory) as territory,
	lower(da.territory_code) as territory_code,
	lower(da.country) as country,
	lower(da.country_code) as country_code,
	lower(da.country_3code) as country_3code,
	lower(da.geographic_zone) as geographic_zone,
	lower(da.continent) as continent,
	da.latitude as latitude,
	da.longitude as longitude,
	da.location_marker as location_marker,
	da.slug as slug,
	'active' as status_code,
	lower(da.language_code) as language_code,
	da.data_source_count as data_source_count,
	da.hash_key as hash_key,
	da.dw_address_id as dw_address_id
from masterdata_hk.dm_address da 
left join reference.ref_hk_address_hierarchy_subarea ah on da.city = ah.city and da.city_area = ah.city_area and da.city_subarea = ah.corrected_city_subarea 
where da.address_type = 'point' or (da.address_type = 'locality' and da.address_type_attribute in ('project', 'street', 'building'))
and da.dw_address_id not in (select dw_address_id from masterdata_hk.address)



call api.update_table_sql('hk', 'masterdata_hk', 'address');


select api.get_normalized_name('cn/hk/nttptp////173 ka king lane')

UPDATE masterdata_hk.address 
SET address_dwid = md5(country_code||'__'||'address'||'__'||id) 
WHERE address_dwid isnull ;

UPDATE masterdata_hk.address 
SET slug = api.clean_slug(country_code||'/'||'hk'||'/'||city_code||''||coalesce(city_area_code,'')||''||
coalesce(city_subarea_code,'')||'/'||coalesce(development,'')||'/'||coalesce(development_phase,'')||'/'||
coalesce(address_building,'')||'/'||coalesce(address_street_text,'')) 
WHERE slug isnull ;



-- DROP TABLE masterdata_hk.address;

CREATE TABLE masterdata_hk.address (
	id bigserial NOT NULL,
	address_dwid text NULL,
	address_type_code text NULL,
	address_type_attribute text NULL,
	full_address_text text NULL,
	address_display_text text NULL,
	address_building text NULL,
	address_number text NULL,
	address_number_range text NULL,
	address_number_range_start text NULL,
	address_number_range_end text NULL,
	address_street_text text NULL,
	street_type text NULL,
	street_prefix text NULL,
	street_prefix_type text NULL,
	street_side text NULL,
	street_name text NULL,
	street_name_root text NULL,
	street_suffix text NULL,
	postal_code_ext text NULL,
	postal_code text NULL,
	development text NULL,
	development_code text NULL,
	development_phase text NULL,
	development_phase_number text NULL,
	neighborhood_block text NULL,
	neighborhood_block_code text NULL,
	neighborhood_section text NULL,
	neighborhood text NULL,
	neighborhood_code text NULL,
	neighborhood_group text NULL,
	city_subarea text NULL,
	city_subarea_id text NULL,
	city_subarea_code text NULL,
	city_district text NULL,
	city_area text NULL,
	city_area_id text NULL,
	city_area_code text NULL,
	city text NULL,
	city_id text NULL,
	city_code text NULL,
	metro_area text NULL,
	metro_area_code text NULL,
	metro_area_district text NULL,
	region_admin_subdistrict text NULL,
	region_admin_subdistrict_code text NULL,
	region_admin_district text NULL,
	region_admin_district_id text NULL,
	region_admin_district_code text NULL,
	region_state_province text NULL,
	region_state_province_code text NULL,
	region_state_province_alias text NULL,
	territory text NULL,
	territory_code text NULL,
	country text NULL,
	country_code text NULL,
	country_3code text NULL,
	geographic_zone text NULL,
	continent text NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	location_marker text NULL,
	slug text NULL,
	status_code text NULL,
	language_code text NULL,
	data_source_count int8 NULL,
	hash_key text NULL,
	dw_address_id text NULL,
	CONSTRAINT address_pk PRIMARY KEY (id),
	CONSTRAINT address_un_dwid UNIQUE (address_dwid),
	CONSTRAINT address_un_slug UNIQUE (slug)
);


CREATE UNIQUE INDEX address_un_dwid ON masterdata_hk.address USING btree (address_dwid);

CREATE UNIQUE INDEX address_un_slug ON masterdata_hk.address USING btree (slug);

ALTER TABLE masterdata_hk.address ADD CONSTRAINT address_fk_address_type_code FOREIGN KEY (address_type_code) REFERENCES masterdata_type.address_type(address_type_code);




--2.project

call api.update_table_sql('hk', 'masterdata_hk', 'project');


select *
from masterdata_hk.dm_project dp 
limit 500



select *
from masterdata_sg.project p 
limit 500


with base as (
select dw_project_id, property_type_id = '200000000' as is_commercial, count(distinct dw_property_id) as unit_count
from masterdata_hk.dm_property dp
group by 1,2
)
, base2 as (
select dw_project_id, count(distinct dw_property_id) as total_unit_count
from masterdata_hk.dm_property dp
group by 1
), temp_base as (
select 
	dw_project_id,
	sum(case when is_commercial is False then unit_count end) as residential_unit_count,
	sum(case when is_commercial is True then unit_count end) as commercial_unit_count
from base
group by 1
)
,final as (
select 
	a.dw_project_id, 
	total_unit_count as unit_count,
	residential_unit_count,
	commercial_unit_count
from temp_base a
left join base2 b using (dw_project_id)
)
select dw_project_id, count(*)
from final
group by 1 having count(*) > 1
;

select trim(initcap(reverse(split_part(reverse(address_display_text), ',', 4)||' ,'||
		split_part(reverse(address_display_text), ',', 3)||' ,'||
		split_part(reverse(address_display_text), ',', 2))))
from masterdata_hk.dm_project dp 
where dw_project_id = '5cdedbdfa2ff147945c8e2cffbcba7bb'
;

DELETE FROM masterdata_hk.project 
WHERE id notnull;


insert into masterdata_hk.project 
(
	address_dwid,developer_dwid,lot_group_dwid,project_type_code,project_display_name,project_name_text,
	completion_year,ownership_type_code,tenure_code,unit_count,residential_unit_count,commercial_unit_count,
	address_display_text,location_display_text,location_marker,status_code,country_code,is_active, dw_address_id , dw_project_id , original_slug 
)
with base as (
	select 	
		dw_project_id, 
		property_type_id = '200000000' as is_commercial, 
		count(distinct dw_property_id) as unit_count
	from masterdata_hk.dm_property dp
	group by 1,2
)
, base2 as (
	select 
		dw_project_id, 
		count(distinct dw_property_id) as total_unit_count
	from masterdata_hk.dm_property dp
	group by 1
), temp_base as (
	select 
		dw_project_id,
		sum(case when is_commercial is False then unit_count end) as residential_unit_count,
		sum(case when is_commercial is True then unit_count end) as commercial_unit_count
	from base
	group by 1
)
, unit_final as (
	select 
		a.dw_project_id, 
		total_unit_count as unit_count,
		residential_unit_count,
		commercial_unit_count
	from temp_base a
		left join base2 b using (dw_project_id)
)
select
	--id,
	--project_dwid,
	a.address_dwid as address_dwid,
	null as developer_dwid,
	null as lot_group_dwid,
	case when dp.property_type_id = '120600000' then 'condo'
		when dp.property_type_id = '120900010' then 'hos'
		when dp.property_type_id = '200000000' then 'commercial'
	end as project_type_code,
	--project_name,
	initcap(dp.project_name) as project_display_name,
	lower(dp.project_name) as project_name_text,
	dp.completion_year as completion_year,
	null as ownership_type_code,
	'leasehold' as tenure_code,
	u.unit_count as unit_count,
	u.residential_unit_count as residential_unit_count,
	u.commercial_unit_count as commercial_unit_count,
	initcap(dp.address_display_text) as address_display_text,
	trim(initcap(reverse(split_part(reverse(dp.address_display_text), ',', 4)||' ,'||
		split_part(reverse(dp.address_display_text), ',', 3)||' ,'||
		split_part(reverse(dp.address_display_text), ',', 2)))) as location_display_text,
	dp.location_marker as location_marker,
	null as status_code,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	dp.dw_address_id ,
	dp.dw_project_id ,
	dp.slug as original_slug
from masterdata_hk.dm_project dp 
left join masterdata_hk.address a on dp.dw_address_id = a.dw_address_id 
left join unit_final u on dp.dw_project_id = u.dw_project_id
;



call api.update_table_sql('hk', 'masterdata_hk', 'project');


UPDATE masterdata_hk.project 
SET project_name = api.get_normalized_name(project_name_text) 
WHERE project_name isnull ;


--UPDATE masterdata_hk.project 
--SET slug = api.clean_slug(country_code||'/'||address_dwid||'/'||project_type_code||'/'||project_name||'-'||id) 
--WHERE slug isnull ;
 
UPDATE masterdata_hk.project 
SET project_dwid = md5(country_code||'__'||'project'||'__'||id) WHERE project_dwid isnull ;


UPDATE masterdata_hk.project 
SET slug = api.clean_slug(country_code||'/'||address_dwid||'/'||coalesce(project_type_code,'')
||'/'||coalesce(project_name,'')||'-'||id) 
WHERE slug isnull ;
 

ALTER TABLE masterdata_hk.project ADD CONSTRAINT project_fk_project_type_code FOREIGN KEY (project_type_code) REFERENCES masterdata_type.project_type(project_type_code);

ALTER TABLE masterdata_hk.project ADD CONSTRAINT project_to_address_fk FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);



--3.building 


select *
from masterdata_hk.dm_building db 
limit 500



select *
from masterdata_sg.building b 
limit 500



select project_name , location_display_text , count(*)
from masterdata_sg.project p 
group by 1, 2 having count(*) > 1


select project_name , location_display_text , count(*)
from masterdata_hk.project p 
group by 1, 2 having count(*) > 1


select *
from masterdata_hk.project p 
where project_name = 'mei-foo-sun-chuen'
;
'''
on-wo-yuen	Kowloon, Kowloon City, To Kwa Wan	2
celestial-heights	Kowloon, Kowloon City, Ho Man Tin	2
palm-springs	New Territories, Yuen Long, Yuen Long	2
fairview-park	New Territories, Yuen Long, Yuen Long	2
mei-foo-sun-chuen	Kowloon, Sham Shui Po, Lai Chi Kok	2


--> drop dups project - a project with multi address street for different phase, but actually it is one project
'''

-- 3052576	5d96f9cb69dbe8e8f3b669ecc9fdbd0c	48ba26f1db0a9ad15458f4e3e13969d7			condo	siena	Siena	siena	2002		leasehold	1063	1063		Siena,Lantau Island - Discovery Bay,Islands,New Territories,Hong Kong (Sar)	Lantau Island - Discovery Bay,Islands,New Territories,Hong Kong (Sar)	POINT (114.010688781738 22.3076648712158)		cn/48ba26f1db0a9ad15458f4e3e13969d7/condo/siena-3052576	cn	true



insert into masterdata_hk.building 
(
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_type_code,building_display_name,building_name_text,
	floors_above_ground,construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	location_marker,country_code,is_active,development,development_phase,dw_address_id,dw_building_id,dw_project_id,original_slug
)



with base as (
	select 	
		dw_building_id , 
		property_type_id = '200000000' as is_commercial, 
		count(distinct dw_property_id) as unit_count
	from masterdata_hk.dm_property dp
	group by 1,2
)
, base2 as (
	select 
		dw_building_id, 
		count(distinct dw_property_id) as total_unit_count
	from masterdata_hk.dm_property dp
	group by 1
), temp_base as (
	select 
		dw_building_id,
		sum(case when is_commercial is False then unit_count end) as residential_unit_count,
		sum(case when is_commercial is True then unit_count end) as commercial_unit_count
	from base
	group by 1
)
, unit_final as (
	select 
		a.dw_building_id, 
		total_unit_count as unit_count,
		residential_unit_count,
		commercial_unit_count
	from temp_base a
		left join base2 b using (dw_building_id)
)
, test as (
select
	--id,
	--building_dwid,
	a.address_dwid as address_dwid,
	p.project_dwid as project_dwid,
	null as lot_group_dwid,
	-- building_name,
	db.block_num as building_block_number,
	null as building_type_code,
	initcap(db.building_name) as building_display_name,
	lower(db.building_name) as building_name_text,
	db.floors_above_ground::int as floors_above_ground,
	construction_end_year, -- need update with phase!!!
	u.unit_count as unit_count,
	u.residential_unit_count as residential_unit_count,
	u.commercial_unit_count as commercial_unit_count,
	lower(db.address_display_text) as address_display_text,
	st_setsrid(st_makepoint(db.longitude, db.latitude), 4326) as location_marker,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	-- columns below are only for testing
	a.development,
	a.development_phase,
	db.dw_address_id,
	db.dw_building_id,
	db.dw_project_id,
	db.slug as original_slug
from masterdata_hk.dm_building db
left join masterdata_hk.address a on db.dw_address_id = a.dw_address_id 
left join masterdata_hk.project p on db.dw_project_id = p.dw_project_id 
--left join masterdata_hk.project p on split_part(db.slug, '/', 4) = replace(p.project_name, '-''-', '-') and split_part(db.slug, '/', 3) = split_part(a.slug, '/', 3)
left join unit_final u on db.dw_building_id = u.dw_building_id -- 63370
)
select * from test where dw_address_id notnull and address_dwid isnull; -- 170 why these address records not exist in dm_address?
select * from test where project_dwid isnull and development notnull; -- 37 --> add missing project-address record and project record --> 1.need to manually fix for estate 'siena' and 'chianti'



call api.update_table_sql('hk', 'masterdata_hk', 'building');

UPDATE masterdata_hk.building 
SET building_name = api.get_normalized_name(building_name_text) 
WHERE building_name isnull ;


UPDATE masterdata_hk.building 
SET building_dwid = md5(country_code||'__'||'building'||'__'||id) WHERE building_dwid isnull ;


ALTER TABLE masterdata_hk.building ADD CONSTRAINT building_to_address_fk FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);

ALTER TABLE masterdata_hk.building ADD CONSTRAINT building_to_project_fk FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);


UPDATE masterdata_hk.building 
SET slug = api.clean_slug(country_code||'/'||address_dwid||'/'||coalesce(building_name,'')||'-'||id) 
WHERE slug isnull ;
 


--4.property


select *
from masterdata_hk.dm_property
limit 500



select *
from masterdata_sg.property b 
limit 500



DROP TABLE masterdata_hk.property;

CREATE TABLE masterdata_hk.property (
	id serial not null,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	unitgroup_dwid text NULL,
	project_dwid text NULL,
	property_type_code text NULL,
	property_name text NULL,
	address_unit text NULL,
	address_floor_text text NULL,
	address_floor_num int4 NULL,
	address_stack text NULL,
	address_stack_num int4 NULL,
	ownership_type_code text NULL,
	bedroom_count int2 NULL,
	bathroom_count int2 NULL,
	other_room_count int4 NULL,
	net_floor_area_sqm numeric NULL,
	gross_floor_area_sqm numeric NULL,
	slug text NULL,
	country_code text NULL,
	is_active bool NULL,
	dw_property_id text NULL,
	dw_address_id text NULL,
	dw_building_id text NULL,
	dw_project_id text NULL,
	original_slug text NULL,
	CONSTRAINT property_pk2 PRIMARY KEY (id),
	CONSTRAINT property_un_dwid2 UNIQUE (property_dwid)
);
ALTER TABLE masterdata_hk.property OWNER TO pipeline;
GRANT ALL ON TABLE masterdata_hk.property TO pipeline;


insert into masterdata_hk.property 
(
	address_dwid,building_dwid,unitgroup_dwid,project_dwid,property_type_code,property_name,
	address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,ownership_type_code,
	bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	country_code,is_active,dw_property_id ,dw_address_id ,dw_building_id ,dw_project_id, original_slug
)
select
	--id,
	--property_dwid,
	b.address_dwid,
	b.building_dwid,
	null as unitgroup_dwid,
	b.project_dwid,
	case when dp.property_type_id = '120600000' then 'condo'
		when dp.property_type_id = '120900010' then 'hos'
		when dp.property_type_id = '200000000' then 'comm'
	end as property_type_code,
	null as property_name,
	dp.address_unit,
	address_floor as address_floor_text,
	dp.floor_seq as address_floor_num,
	dp.address_stack,
	null as address_stack_num,
	null as ownership_type_code,
	dp.bedroom_count,
	dp.bathroom_count,
	dp.other_room_count,
	dp.net_floor_area_sqm,
	dp.gross_floor_area_sqm,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	dp.dw_property_id,
	dp.dw_address_id,
	dp.dw_building_id,
	dp.dw_project_id,
	dp.slug as original_slug
from masterdata_hk.dm_property dp 
left join masterdata_hk.building b on dp.dw_building_id = b.dw_building_id 
--limit 100
; -- 2,369,667



call api.update_table_sql('hk', 'masterdata_hk', 'property');

UPDATE masterdata_hk.property 
SET property_dwid = md5(country_code||'__'||'property'||'__'||id) WHERE property_dwid isnull ;

UPDATE masterdata_hk.property 
SET slug = api.clean_slug(country_code||'/'||property_type_code||'/'||building_dwid||'/'||address_unit||'-'||id) 
WHERE slug isnull ;
 

ALTER TABLE masterdata_hk.property ADD CONSTRAINT property_fk_address_dwid2 FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);

ALTER TABLE masterdata_hk.property ADD CONSTRAINT property_fk_building_dwid2 FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);

ALTER TABLE masterdata_hk.property ADD CONSTRAINT property_fk_project_dwid2 FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);

ALTER TABLE masterdata_hk.property ADD CONSTRAINT property_fk_type_code2 FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);


ALTER TABLE masterdata_hk.sale_transaction drop CONSTRAINT sale_transaction_fk_property_dwid;
ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sale_transaction_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid)
--ALTER TABLE map_hk.midland_unit_to_dwid drop CONSTRAINT midland_building_to_dwid_fk_property_dwid3;
--ALTER TABLE map_hk.midland_unit_to_dwid ADD CONSTRAINT midland_building_to_dwid_fk_property_dwid3 FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid)


DROP TABLE masterdata_hk.property_v1;






--delete from masterdata_hk.property
--where id notnull; -- 

select count(*) from masterdata_hk.property;
select count(*) from masterdata_hk.dm_property;
select * from masterdata_hk.property limit 10;

select dw_property_id 
from masterdata_hk.dm_property a
except
select dw_property_id
from masterdata_hk.dm_property_v1 b
;


update masterdata_hk.property a
set (dw_address_id, dw_building_id , dw_project_id )= 
(select dw_address_id, dw_building_id , dw_project_id from masterdata_hk.dm_property b
where a.dw_property_id = b.dw_property_id)
where a.dw_property_id notnull;


update masterdata_hk.property a
set (address_unit, address_floor_text, address_floor_num, address_stack, bedroom_count, bathroom_count, other_room_count, net_floor_area_sqm, gross_floor_area_sqm, original_slug)= 
(select address_unit, address_floor, floor_seq, address_stack, bedroom_count, bathroom_count, other_room_count, net_floor_area_sqm, gross_floor_area_sqm, slug  from masterdata_hk.dm_property b
where a.dw_property_id = b.dw_property_id)
where a.dw_property_id notnull;




--5.sale_transaction
select *
from masterdata_hk.dm_property_activity dpa 
limit 100


select *
from masterdata_sg.sale_transaction st 
limit 100



DROP TABLE masterdata_hk.sale_transaction;

CREATE TABLE masterdata_hk.sale_transaction (
	id serial NOT NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_name text NULL,
	units_sold int4 NULL,
	sale_type text NULL,
	sale_subtype text NULL,
	property_completion_year int2 NULL,
	property_type_code text NULL,
	address_unit text NULL,
	address_local_text text NULL,
	tenure_code text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	contract_date date NULL,
	settlement_date date NULL,
	purchase_amount numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	CONSTRAINT sale_transaction_pk PRIMARY KEY (id),
	CONSTRAINT sale_transaction_un_dwid UNIQUE (activity_dwid)
);
CREATE INDEX sale_transaction_data_uuid_idx ON masterdata_hk.sale_transaction USING btree (data_uuid);

-- Permissions

ALTER TABLE masterdata_hk.sale_transaction OWNER TO pipeline;
GRANT ALL ON TABLE masterdata_hk.sale_transaction TO pipeline;


select date_part('year', '1985-11-22'::date) 




insert into masterdata_hk.sale_transaction 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,units_sold,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,unit_price_psm,contract_date,settlement_date,purchase_amount,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
)
with completion_year_base as (
	select building_id, date_part('year', building_first_op_date::date) as completion_year, count(*) as ct
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
, completion_year_dedup as (
select building_id, completion_year, row_number() over(partition by building_id order by ct desc) as seq
from completion_year_base
)
select 
	--id,
	--activity_dwid,
	p.property_dwid,
	p.address_dwid,
	p.building_dwid,
	p.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	null as activity_name,
	1::int as units_sold,
	case when dpa.activity_sub_type = 'new sale' then 'new-sale'
		else 'resale'
		end as sale_type,
	null as sale_subtype,
	cy.completion_year as property_completion_year,
	p.property_type_code as property_type_code,
	p.address_unit as address_unit,
	a.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count as bathroom_count,
	p.bedroom_count as bedroom_count,
	p.gross_floor_area_sqm as gross_floor_area_sqm,
	p.net_floor_area_sqm as net_floor_area_sqm,
	(dpa.activity_amount/p.gross_floor_area_sqm)::int as unit_price_psm,
	mid.tx_date as contract_date,
	dpa.activity_date as settlement_date, -- update_date
	dpa.activity_amount as purchase_amount,
	'cn' as country_code,
	dpa.activity_source_id::uuid as data_uuid,
	null as data_source_uuid,
	'hk_midland_transaction' as data_source,
	dpa.dw_property_id ,
	dpa.slug as original_slug
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_dedup cy on mid.building_id = cy.building_id and cy.seq = 1
where dpa.activity_type = 'sale_transaction'
; -- 1,606,797



UPDATE masterdata_hk.sale_transaction 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; -- 1,606,797



ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sale_transaction_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);

ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sale_transaction_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);

ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sale_transaction_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);

ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sale_transaction_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);

ALTER TABLE masterdata_hk.sale_transaction ADD CONSTRAINT sg_sale_transaction_fk_property_dwid FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);


update masterdata_hk.sale_transaction 
set land_area_sqm = null 
where land_area_sqm notnull;




insert into masterdata_hk.sale_transaction 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,units_sold,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,contract_date,settlement_date,purchase_amount,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
)
with completion_year_base as (
	select building_id, date_part('year', building_first_op_date::date) as completion_year, count(*) as ct
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
, completion_year_dedup as (
select building_id, completion_year, row_number() over(partition by building_id order by ct desc) as seq
from completion_year_base
)
select 
	--id,
	--activity_dwid,
	p.property_dwid,
	p.address_dwid,
	p.building_dwid,
	p.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	null as activity_name,
	1::int as units_sold,
	case when dpa.activity_sub_type = 'new sale' then 'new-sale'
		else 'resale'
		end as sale_type,
	null as sale_subtype,
	cy.completion_year as property_completion_year,
	p.property_type_code as property_type_code,
	p.address_unit as address_unit,
	a.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count as bathroom_count,
	p.bedroom_count as bedroom_count,
	p.gross_floor_area_sqm as gross_floor_area_sqm,
	p.net_floor_area_sqm as net_floor_area_sqm,
	null as land_area_sqm,
	mid.tx_date as contract_date,
	dpa.activity_date as settlement_date, -- update_date
	dpa.activity_amount as purchase_amount,
	'cn' as country_code,
	dpa.activity_source_id::uuid as data_uuid,
	null as data_source_uuid,
	'hk_midland_transaction' as data_source,
	dpa.dw_property_id ,
	dpa.slug as original_slug
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_dedup cy on mid.building_id = cy.building_id and cy.seq = 1
where dpa.activity_type = 'sale_transaction'

; -- 



select count(dpa2.*) from masterdata_hk.dm_property_activity dpa1
left join masterdata_hk.dm_property_activity_v1 dpa2 using (dw_activity_id)
where dpa1.activity_type = 'sale_transaction' and dpa2.activity_type = 'sale_transaction'
and dpa2.dw_activity_id isnull; -- 0


select dw_activity_id
from masterdata_hk.dm_property_activity a
where a.activity_type = 'sale_transaction'
except
select dw_activity_id
from masterdata_hk.dm_property_activity_v1 b
where b.activity_type = 'sale_transaction'
; -- 237,038


with id_base as (
	select dw_activity_id
	from masterdata_hk.dm_property_activity a
	where a.activity_type = 'sale_transaction'
	except
	select dw_activity_id
	from masterdata_hk.dm_property_activity_v1 b
	where b.activity_type = 'sale_transaction'
)
, completion_year_base as (
	select building_id, date_part('year', building_first_op_date::date) as completion_year, count(*) as ct
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
, completion_year_dedup as (
select building_id, completion_year, row_number() over(partition by building_id order by ct desc) as seq
from completion_year_base
)
, final as (
select 
	--id,
	--activity_dwid,
	p.property_dwid,
	p.address_dwid,
	p.building_dwid,
	p.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	null as activity_name,
	1::int as units_sold,
	case when dpa.activity_sub_type = 'new sale' then 'new-sale'
		else 'resale'
		end as sale_type,
	null as sale_subtype,
	cy.completion_year as property_completion_year,
	p.property_type_code as property_type_code,
	p.address_unit as address_unit,
	a.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count as bathroom_count,
	p.bedroom_count as bedroom_count,
	p.gross_floor_area_sqm as gross_floor_area_sqm,
	p.net_floor_area_sqm as net_floor_area_sqm,
	null as land_area_sqm,
	mid.tx_date as contract_date,
	dpa.activity_date as settlement_date, -- update_date
	dpa.activity_amount as purchase_amount,
	'cn' as country_code,
	dpa.activity_source_id::uuid as data_uuid,
	null as data_source_uuid,
	'hk_midland_transaction' as data_source,
	dpa.dw_property_id ,
	dpa.slug as original_slug
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_dedup cy on mid.building_id = cy.building_id and cy.seq = 1
where dpa.dw_activity_id in (select dw_activity_id from id_base)
)
select count(*) -- 237038
from final 
;


insert into masterdata_hk.sale_transaction 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,units_sold,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,contract_date,settlement_date,purchase_amount,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
)
with id_base as (
	select dw_activity_id
	from masterdata_hk.dm_property_activity a
	where a.activity_type = 'sale_transaction'
	except
	select dw_activity_id
	from masterdata_hk.dm_property_activity_v1 b
	where b.activity_type = 'sale_transaction'
)
, completion_year_base as (
	select building_id, date_part('year', building_first_op_date::date) as completion_year, count(*) as ct
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
, completion_year_dedup as (
select building_id, completion_year, row_number() over(partition by building_id order by ct desc) as seq
from completion_year_base
)
select 
	--id,
	--activity_dwid,
	p.property_dwid,
	p.address_dwid,
	p.building_dwid,
	p.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	null as activity_name,
	1::int as units_sold,
	case when dpa.activity_sub_type = 'new sale' then 'new-sale'
		else 'resale'
		end as sale_type,
	null as sale_subtype,
	cy.completion_year as property_completion_year,
	p.property_type_code as property_type_code,
	p.address_unit as address_unit,
	a.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count as bathroom_count,
	p.bedroom_count as bedroom_count,
	p.gross_floor_area_sqm as gross_floor_area_sqm,
	p.net_floor_area_sqm as net_floor_area_sqm,
	null as land_area_sqm,
	mid.tx_date as contract_date,
	dpa.activity_date as settlement_date, -- update_date
	dpa.activity_amount as purchase_amount,
	'cn' as country_code,
	dpa.activity_source_id::uuid as data_uuid,
	null as data_source_uuid,
	'hk_midland_transaction' as data_source,
	dpa.dw_property_id ,
	dpa.slug as original_slug
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_dedup cy on mid.building_id = cy.building_id and cy.seq = 1
where dpa.dw_activity_id in (select dw_activity_id from id_base)
;


UPDATE masterdata_hk.sale_transaction 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; 




insert into masterdata_hk.sale_transaction 
(
	units_sold,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,contract_date,settlement_date,purchase_amount,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
)
select
	1::int as units_sold,
	case when mid.mkt_type = 1 then 'new-sale'
		when mid.mkt_type = 2 then 'resale'
		end as sale_type,
	null as sale_subtype,
	date_part('year', mid.building_first_op_date::date) as property_completion_year,
	null as property_type_code,
	mid.floor ||'-'||mid.flat as address_unit,
	null as address_local_text,
	'leasehold' as tenure_code,
	null as bathroom_count,
	mid.bedroom as bedroom_count,
	mid.area_sqm as gross_floor_area_sqm,
	mid.net_area_sqm as net_floor_area_sqm,
	null as land_area_sqm,
	mid.tx_date as contract_date,
	mid.update_date as settlement_date,
	mid.price as purchase_amount,
	'cn' as country_code,
	mid.data_uuid as data_uuid,
	null as data_source_uuid,
	'hk_midland_transaction' as data_source,
	null as dw_property_id ,
	null as original_slug
from map_hk.midland_sale_txn__map txn_map
left join source.hk_midland_realty_sale_transaction mid on txn_map.data_uuid::text = mid.data_uuid::text
where txn_map.activity_dwid isnull


UPDATE masterdata_hk.sale_transaction 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; 


update masterdata_hk.sale_transaction a
set property_dwid = null;


update masterdata_hk.sale_transaction a
set property_dwid = 
(select property_dwid from masterdata_hk.property b
where a.dw_property_id= b.dw_property_id)
where a.dw_property_id notnull


create table playground.temp_sale_transaction_id as 
select a.*, b.property_dwid as new_prop_dwid
from masterdata_hk.sale_transaction a
left join masterdata_hk.property b
on a.dw_property_id= b.dw_property_id
where a.property_dwid  isnull
;


select *
from playground.temp_sale_transaction_id
limit 100


select count(*)
from playground.temp_sale_transaction_id


update masterdata_hk.sale_transaction a
set property_dwid = 
(select b.new_prop_dwid from playground.temp_sale_transaction_id b 
where a.id = b.id)
where a.property_dwid isnull
;


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.sale_transaction_new st 
; --0.86537484912324821726


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.sale_transaction_backup st 
; --0.88801323378124305684



CREATE INDEX sale_transaction_data_uuid_idx2 ON masterdata_hk.sale_transaction_new USING btree (data_uuid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_p2k PRIMARY KEY (id);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_un_dwid2 UNIQUE (activity_dwid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_fk_address_dwid2 FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_fk_building_dwid2 FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_fk_project_dwid2 FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sale_transaction_fk_property_dwid2 FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);

ALTER TABLE masterdata_hk.sale_transaction_new ADD CONSTRAINT sg_sale_transaction_fk_property_dwid2 FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);



ALTER TABLE map_hk.midland_sale_txn__map drop CONSTRAINT midland_sale_txn__map_fk_activity_dwid;

ALTER TABLE map_hk.midland_sale_txn__map ADD CONSTRAINT  midland_sale_txn__map_fk_activity_dwid2
    references masterdata_hk.sale_transaction_new (activity_dwid);


drop table masterdata_hk.sale_transaction cascade;
   

--6.project_attribute

insert into masterdata_hk.project_attribute 
(
	project_dwid, attribute_name, attribute_value
)
with completion_year_base as (
	select estate_id, building_first_op_date as completion_date, row_number() over(partition by estate_id order by building_first_op_date) as seq
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
), project_attribute_1 as (
select p.project_dwid , cy.completion_date as tenure_start_date
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_base cy on mid.estate_id = cy.estate_id and cy.seq = 1
where p.project_dwid notnull and cy.completion_date notnull
group by 1,2  -- 1,314
)
select project_dwid, 'tenure_start_date' as attribute_name, tenure_start_date::text as attribute_value
from project_attribute_1



--7.map
-- -- 1.. midland_sale_transaction__map

select *
from masterdata_hk.dm_property_activity dpa 
limit 100



select *
from map_sg.ura_sale__map usm 
limit 100



select 
	st.data_uuid::uuid as data_uuid, 
	st.activity_dwid ,
	st.property_dwid , 
	st.building_dwid ,
	st.address_dwid ,
	st.project_dwid 
from masterdata_hk.sale_transaction st 
left join "source".hk_midland_realty_sale_transaction mst on st.data_uuid = mst.data_uuid 
where mst.data_uuid notnull
-- should revert the join order


drop TABLE map_hk.midland_sale_txn__map;

CREATE TABLE map_hk.midland_sale_txn__map (
	data_uuid uuid NOT NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	land_parcel_dwid text NULL,
	lot_group_dwid text NULL,
	status_code varchar NULL,
	CONSTRAINT midland_sale_txn__map_pk PRIMARY KEY (data_uuid),
	CONSTRAINT midland_sale_txn__map_fk_activity_dwid FOREIGN KEY (activity_dwid) REFERENCES masterdata_hk.sale_transaction(activity_dwid),
	CONSTRAINT midland_sale_txn__map_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid),
	CONSTRAINT midland_sale_txn__map_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid),
	CONSTRAINT midland_sale_txn__map_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid),
	CONSTRAINT midland_sale_txn__map_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid)
);


insert into map_hk.midland_sale_txn__map
(
	data_uuid,activity_dwid,property_dwid,building_dwid,address_dwid,project_dwid,land_parcel_dwid,lot_group_dwid,status_code
)
select 
	mst.data_uuid::uuid as data_uuid, 
	st.activity_dwid ,
	st.property_dwid , 
	st.building_dwid ,
	st.address_dwid ,
	st.project_dwid ,
	null as land_parcel_dwid,
	null as lot_group_dwid,
	null as status_code
from "source".hk_midland_realty_sale_transaction mst
left join masterdata_hk.sale_transaction st on st.data_uuid = mst.data_uuid
where mst.data_uuid notnull
; -- 244,380



select *
from map_sg.ura_project_name_to_dwid upntd 
limit 100



select *
from "source".hk_midland_realty_sale_transaction mst
where mst.data_uuid notnull
limit 100



select distinct 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id ,	building_name , building_id 
	---,	floor, flat , unit_id -- 234,837
from "source".hk_midland_realty_sale_transaction mst
where mst.data_uuid notnull
; -- 18,648



update map_hk.midland_sale_txn__map txn_map
set activity_dwid = 
(select txn.activity_dwid from masterdata_hk.sale_transaction txn
where txn_map.data_uuid = txn.data_uuid 
)
where txn_map.activity_dwid isnull; -- 4020





update masterdata_hk.building b 
set address_dwid = 
(select a.address_dwid from address_base a where a.building_dwid = b.building_dwid )
where b.address_dwid isnull
; -- 170


------------


CREATE TABLE map_hk.midland_building_to_dwid (
	id int4 NOT NULL DEFAULT nextval('metadata.id_seq'::regclass),
	region_name text NULL,
	region_id text NULL,
	district_name text NULL,
	district_id text NULL,
	subregion_name text NULL,
	subregion_id text NULL,
	estate_name text NULL,
	estate_id text NULL,
	phase_name text NULL,
	phase_id text NULL,
	building_name text NULL,
	building_id text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	project_name text NULL,
	project_display_name text NULL,
	status_code text NULL,
	CONSTRAINT midland_building_to_dwid_pk PRIMARY KEY (id),
	CONSTRAINT midland_building_to_dwid_un UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name),
	CONSTRAINT midland_building_to_dwid_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid),
	CONSTRAINT midland_building_to_dwid_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid),
	CONSTRAINT midland_building_to_dwid_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid)
);


insert into map_hk.midland_building_to_dwid
(
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id ,	building_name , building_id ,
	building_dwid, address_dwid, project_dwid, lot_group_dwid , project_name , project_display_name, status_code 
)
with id_base as (
select mst.building_id , mp.building_dwid , mp.address_dwid , mp.project_dwid, row_number() over(partition by mst.building_id order by mp.building_dwid) as seq
from map_hk.midland_sale_txn__map mp
left join "source".hk_midland_realty_sale_transaction mst using (data_uuid)
group by 1,2,3,4 order by 1
)
, midland_base as (
select distinct 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , mst.building_id, 
	row_number() over(partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name order by mst.building_id) as seq
from "source".hk_midland_realty_sale_transaction mst -- 244,380
)
select 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , mst.building_id ,
	id.building_dwid, id.address_dwid, id.project_dwid, null as lot_group_dwid , p.project_name , p.project_display_name, null as status_code 
from midland_base mst
left join id_base id on mst.building_id = id.building_id and id.seq = 1
left join masterdata_hk.project p on id.project_dwid = p.project_dwid 
where mst.seq = 1
; -- 18569



CREATE TABLE map_hk.midland_unit_to_dwid (
	id int4 NOT NULL DEFAULT nextval('metadata.id_seq'::regclass),
	region_name text NULL,
	region_id text NULL,
	district_name text NULL,
	district_id text NULL,
	subregion_name text NULL,
	subregion_id text NULL,
	estate_name text NULL,
	estate_id text NULL,
	phase_name text NULL,
	phase_id text NULL,
	building_name text NULL,
	building_id text NULL,
	floor text NULL,
	flat text NULL,
	unit_id text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	project_name text NULL,
	project_display_name text NULL,
	status_code text NULL,
	CONSTRAINT midland_building_to_dwid_pk2 PRIMARY KEY (id),
	CONSTRAINT midland_building_to_dwid_un2 UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, flat),
	CONSTRAINT midland_building_to_dwid_fk_address_dwid2 FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid),
	CONSTRAINT midland_building_to_dwid_fk_building_dwid2 FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid),
	CONSTRAINT midland_building_to_dwid_fk_project_dwid2 FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid),
	CONSTRAINT midland_building_to_dwid_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid)
);


insert into map_hk.midland_unit_to_dwid
(
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id ,	building_name , building_id , floor, flat, unit_id,
	property_dwid, building_dwid, address_dwid, project_dwid, lot_group_dwid , project_name , project_display_name, status_code 
)
select * from map_hk.midland_unit_to_dwid_temp;


drop TABLE map_hk.midland_unit_to_dwid_temp;


CREATE TABLE map_hk.midland_unit_to_dwid_temp as
with id_base as (
select mst.unit_id , mp.property_dwid , mp.building_dwid , mp.address_dwid , mp.project_dwid, row_number() over(partition by mst.unit_id order by mp.property_dwid) as seq
from map_hk.midland_sale_txn__map mp
left join "source".hk_midland_realty_sale_transaction mst using (data_uuid)
group by 1,2,3,4,5 order by 1 -- 235,904
)
, midland_base as (
select distinct 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , building_id, 
	floor, flat, unit_id,
	row_number() over(partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, flat order by unit_id) as seq
from "source".hk_midland_realty_sale_transaction mst -- 244,564
)
select 
	row_number() over () as id,
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , building_id , floor, flat, mst.unit_id,
	id.property_dwid, id.building_dwid, id.address_dwid, id.project_dwid, null as lot_group_dwid , p.project_name , p.project_display_name, null as status_code 
from midland_base mst
left join id_base id on mst.unit_id = id.unit_id and id.seq = 1
left join masterdata_hk.project p on id.project_dwid = p.project_dwid 
where mst.seq = 1 
; -- 234414


ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_fk_address_dwid3 FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_fk_building_dwid3 FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_fk_project_dwid3 FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_fk_property_dwid3 FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_pk3 PRIMARY KEY (id);
ALTER TABLE map_hk.midland_unit_to_dwid_temp ADD CONSTRAINT midland_building_to_dwid_un3 UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, flat);



update map_hk.midland_unit_to_dwid a
set (address_street, address_number) = 
(select street_name, address_number
from masterdata_hk.address b where a.address_dwid = b.address_dwid)
where a.address_dwid notnull



update map_hk.midland_building_to_dwid a
set (address_street, address_number) = 
(select street_name, address_number
from masterdata_hk.address b where a.address_dwid = b.address_dwid)
where a.address_dwid notnull



----> UPDATE:

with id_base as (
select mst.building_id , mp.building_dwid , mp.address_dwid , mp.project_dwid, row_number() over(partition by mst.building_id order by mp.building_dwid) as seq
from map_hk.midland_sale_txn__map mp
left join "source".hk_midland_realty_sale_transaction mst using (data_uuid)
group by 1,2,3,4 order by 1
)
, midland_base as (
select distinct 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , mst.building_id, 
	row_number() over(partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name order by mst.building_id) as seq
from "source".hk_midland_realty_sale_transaction mst -- 244,380
)
select 
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id , building_name , mst.building_id ,
	id.building_dwid, id.address_dwid, id.project_dwid, null as lot_group_dwid , p.project_name , p.project_display_name, null as status_code 
from midland_base mst
left join id_base id on mst.building_id = id.building_id and id.seq = 1
left join masterdata_hk.project p on id.project_dwid = p.project_dwid 
where mst.seq = 1
; -- 18569


select *
from reference.hk_midland_hkpost_correction_backfill hmhcb 
limit 100


with midland_base as (
select distinct 
	mst.region_name , mst.region_id , mst.district_name , mst.district_id , mst.subregion_name , mst.subregion_id ,
	mst.estate_name , mst.estate_id , mst.phase_name , mst.phase_id , mst.building_name , mst.building_id , b.street_num , b.street_name , mst.floor, mst.flat,
    initcap(b.region_name) as city, initcap(b.subregion_name) as city_area, initcap(b.district_name) as city_subarea,
    b.corrected_estate_name, b.corrected_phase_name, b.corrected_building_name, b.corrected_street_num, b.corrected_street_name,
	--row_number() over(partition by mst.region_name, mst.district_name, mst.subregion_name, mst.estate_name, mst.phase_name, mst.building_name order by mst.building_id) as seq
    row_number() over(partition by mst.unit_id order by mst.update_date) as seq
from "source".hk_midland_realty_sale_transaction mst
left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
)
, id_base as (
select b.*, c.dw_address_id, d.dw_building_id , d.dw_project_id , d.dw_property_id
from midland_base b
left join raw_internal.address c
    on f_prep_dw_id(b.city) = f_prep_dw_id(c.city)
    and f_prep_dw_id(b.city_area) = f_prep_dw_id(c.city_area)
    and f_prep_dw_id(b.city_subarea) = f_prep_dw_id(c.city_subarea)
    and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
    and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
    and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
    and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
    and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name) 
left join masterdata_hk.dm_property d 
	on d.dw_address_id = c.dw_address_id
    and f_prep_dw_id(d.address_floor) = f_prep_dw_id(b.floor)
    and f_prep_dw_id(d.address_stack) = f_prep_dw_id(b.flat)
where b.seq = 1 --234,259
)
, check_consistency as (
select base.*, a.address_dwid, b.building_dwid, pj.project_dwid , prop.address_dwid as p_address_dwid, prop.building_dwid as p_building_dwid, prop.project_dwid as p_project_dwid, prop.property_dwid 
from id_base base
left join masterdata_hk.address a on base.dw_address_id = a.dw_address_id 
left join masterdata_hk.building b on base.dw_building_id = b.dw_building_id
left join masterdata_hk.project pj on base.dw_project_id = pj.dw_project_id 
left join masterdata_hk.property prop on base.dw_property_id = prop.dw_property_id 
)
--select address_dwid = p_address_dwid, building_dwid = p_building_dwid, project_dwid = p_project_dwid, count(*)
--from check_consistency
--group by 1,2,3
select a.*, b1.project_name_text , b2.project_name_text as p_project_name_text
from check_consistency a 
left join masterdata_hk.project b1 on a.project_dwid = b1.project_dwid
left join masterdata_hk.project b2 on a.p_project_dwid = b2.project_dwid
where a.project_dwid != a.p_project_dwid
; --> pj.project_dwid is correct one

--> but in that case the project id in property table is not correct / consistent with project --> need to fix the bug



update masterdata_hk.property a
set project_dwid = 
(select b.project_dwid from masterdata_hk.project b
where a.dw_project_id = b.dw_project_id)
where a.project_dwid != b.project_dwid 


select a.project_dwid , b.project_dwid, b.project_name 
from masterdata_hk.property a
left join masterdata_hk.project b on a.dw_project_id = b.dw_project_id
where a.project_dwid != b.project_dwid 
group by 1,2,3


update masterdata_hk.property a
set project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39'
where project_dwid = '997e214983c4009e3baf560fddee5480' -- fairview-park


update masterdata_hk.property a
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0' -- mei-foo-sun-chuen


update masterdata_hk.property a
set project_dwid = '23ab7c0be7319fd9a16022138bfed95b'
where project_dwid = '3441fcbc4ab2d59b2f1f0b2a84445bcd' -- on-wo-yuen


update masterdata_hk.property a
set project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8'
where project_dwid = '5741387ef9965e3352b8f7777aeaab04' -- palm-springs






select f_clone_table('map_hk', 'midland_building_to_dwid', 'map_hk', 'midland_building_to_dwid_backup', TRUE, TRUE);
select f_clone_table('map_hk', 'midland_unit_to_dwid', 'map_hk', 'midland_unit_to_dwid_backup', TRUE, TRUE);
select f_clone_table('map_hk', 'midland_sale_txn__map', 'map_hk', 'midland_sale_txn__map_backup', TRUE, TRUE);

drop table map_hk.midland_building_to_dwid ;
drop table map_hk.midland_unit_to_dwid ;

select *
from map_hk.midland_building_to_dwid_backup mbtdb 
limit 100;


select *
from map_hk.midland_unit_to_dwid_backup mbtdb 
limit 100

-- unit_map:
CREATE TABLE map_hk.midland_unit_to_dwid (
	id int4 NOT NULL DEFAULT nextval('metadata.id_seq'::regclass),
	region_name text NULL,
	region_id text NULL,
	district_name text NULL,
	district_id text NULL,
	subregion_name text NULL,
	subregion_id text NULL,
	estate_name text NULL,
	estate_id text NULL,
	phase_name text NULL,
	phase_id text NULL,
	building_name text NULL,
	building_id text NULL,
	address_number text NULL,
	address_street text NULL,
	floor text NULL,
	stack text NULL,
	unit_id text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	CONSTRAINT midland_unit_to_dwid_pk PRIMARY KEY (id),
	CONSTRAINT midland_unit_to_dwid_un UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack),
	CONSTRAINT midland_unit_to_dwid_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid),
	CONSTRAINT midland_unit_to_dwid_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid),
	CONSTRAINT midland_unit_to_dwid_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid),
	CONSTRAINT midland_unit_to_dwid_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid)
);



insert into map_hk.midland_unit_to_dwid
(
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id ,	building_name , building_id , 
	address_number, address_street, floor, stack, unit_id,
	property_dwid, building_dwid, address_dwid, project_dwid, lot_group_dwid
)
with midland_base as (
select distinct 
	mst.region_name , mst.region_id , mst.district_name , mst.district_id , mst.subregion_name , mst.subregion_id ,
	mst.estate_name , mst.estate_id , mst.phase_name , mst.phase_id , mst.building_name , mst.building_id , b.street_num , b.street_name , mst.floor, mst.flat, mst.unit_id,
    initcap(b.region_name) as city, initcap(b.subregion_name) as city_area, initcap(b.district_name) as city_subarea,
    b.corrected_estate_name, b.corrected_phase_name, b.corrected_building_name, b.corrected_street_num, b.corrected_street_name,
	row_number() over(partition by mst.region_name, mst.district_name, mst.subregion_name, mst.estate_name, mst.phase_name, mst.building_name, mst.floor, mst.flat order by mst.unit_id) as seq
    --row_number() over(partition by mst.unit_id order by mst.update_date) as seq
from "source".hk_midland_realty_sale_transaction mst
left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
)
, id_base as (
select b.*, c.dw_address_id, d.dw_building_id , d.dw_project_id , d.dw_property_id
	--, row_number() over(partition by b.region_name, b.district_name, b.subregion_name, b.estate_name, b.phase_name,
	--b.building_name, b.floor, b.flat order by b.unit_id) as seq
from midland_base b
left join raw_internal.address c
    on f_prep_dw_id(b.city) = f_prep_dw_id(c.city)
    and f_prep_dw_id(b.city_area) = f_prep_dw_id(c.city_area)
    and f_prep_dw_id(b.city_subarea) = f_prep_dw_id(c.city_subarea)
    and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
    and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
    and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
    and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
    and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name) 
left join masterdata_hk.dm_property d 
	on d.dw_address_id = c.dw_address_id
    and f_prep_dw_id(d.address_floor) = f_prep_dw_id(b.floor)
    and f_prep_dw_id(d.address_stack) = f_prep_dw_id(b.flat)
where b.seq = 1 --234,259
-- test UKey：
-- and b.region_name = 'Kowloon' and b.district_name = 'Kwun Tong' and b.subregion_name = 'Kwun Tong' 
-- and b.estate_name = 'Grand Central' and b.phase_name = 'Phase II' and b.building_name = 'Tower 3' and b.floor = '36' and b.flat = 'G';
)
select
	base.region_name,base.region_id,base.district_name,base.district_id,base.subregion_name,base.subregion_id,
	base.estate_name,base.estate_id,base.phase_name,base.phase_id,base.building_name,base.building_id,
	base.street_num,base.street_name,base.floor,base.flat, base.unit_id,
	prop.property_dwid , b.building_dwid, a.address_dwid,  pj.project_dwid , null as lot_group_dwid
from id_base base
left join masterdata_hk.address a on base.dw_address_id = a.dw_address_id 
left join masterdata_hk.building b on base.dw_building_id = b.dw_building_id
left join masterdata_hk.project pj on base.dw_project_id = pj.dw_project_id 
left join masterdata_hk.property prop on base.dw_property_id = prop.dw_property_id 
; -- 235219 --> 235,627 after dedup



-- building map:

CREATE TABLE map_hk.midland_building_to_dwid (
	id int4 NOT NULL DEFAULT nextval('metadata.id_seq'::regclass),
	region_name text NULL,
	region_id text NULL,
	district_name text NULL,
	district_id text NULL,
	subregion_name text NULL,
	subregion_id text NULL,
	estate_name text NULL,
	estate_id text NULL,
	phase_name text NULL,
	phase_id text NULL,
	building_name text NULL,
	building_id text NULL,
	address_number text NULL,
	address_street text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	CONSTRAINT midland_building_to_dwid_pk PRIMARY KEY (id),
	CONSTRAINT midland_building_to_dwid_un UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name),
	CONSTRAINT midland_building_to_dwid_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid),
	CONSTRAINT midland_building_to_dwid_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid),
	CONSTRAINT midland_building_to_dwid_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid)
);



insert into map_hk.midland_building_to_dwid
(
	region_name , region_id , district_name , district_id , subregion_name , subregion_id ,
	estate_name , estate_id , phase_name , phase_id ,	building_name , building_id , 
	address_number, address_street, building_dwid, address_dwid, project_dwid, lot_group_dwid
)
with midland_base as (
select distinct 
	mst.region_name , mst.region_id , mst.district_name , mst.district_id , mst.subregion_name , mst.subregion_id ,
	mst.estate_name , mst.estate_id , mst.phase_name , mst.phase_id , mst.building_name , mst.building_id , b.street_num , b.street_name , mst.floor, mst.flat, mst.unit_id,
    initcap(b.region_name) as city, initcap(b.subregion_name) as city_area, initcap(b.district_name) as city_subarea,
    b.corrected_estate_name, b.corrected_phase_name, b.corrected_building_name, b.corrected_street_num, b.corrected_street_name,
	row_number() over(partition by mst.region_name, mst.district_name, mst.subregion_name, mst.estate_name, mst.phase_name, mst.building_name order by mst.building_id) as seq
    --row_number() over(partition by mst.unit_id order by mst.update_date) as seq
from "source".hk_midland_realty_sale_transaction mst
left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
)
, id_base as (
select b.*, c.dw_address_id, d.dw_building_id , d.dw_project_id , d.dw_property_id
	, row_number() over(partition by b.building_id order by d.dw_building_id, d.dw_project_id, c.dw_address_id) as seq2
from midland_base b
left join raw_internal.address c
    on f_prep_dw_id(b.city) = f_prep_dw_id(c.city)
    and f_prep_dw_id(b.city_area) = f_prep_dw_id(c.city_area)
    and f_prep_dw_id(b.city_subarea) = f_prep_dw_id(c.city_subarea)
    and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
    and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
    and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
    and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
    and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name) 
left join masterdata_hk.dm_property d 
	on d.dw_address_id = c.dw_address_id
    and f_prep_dw_id(d.address_floor) = f_prep_dw_id(b.floor)
    and f_prep_dw_id(d.address_stack) = f_prep_dw_id(b.flat)
where b.seq = 1
-- test dedup:
-- where b.region_name = 'New Territories' and b.district_name = 'Fairview / Palm Springs / The Vineyard' and b.subregion_name = 'Yuen Long'
-- and b.estate_name = 'Fairview Park' and b.phase_name = 'Section B (3Rd Street)' and b.building_name = '6 3Rd Street';
)
select -- distinct
	base.region_name,base.region_id,base.district_name,base.district_id,base.subregion_name,base.subregion_id,
	base.estate_name,base.estate_id,base.phase_name,base.phase_id,base.building_name,base.building_id,
	base.street_num,base.street_name,--base.floor,base.flat, base.unit_id,
	--prop.property_dwid , 
	b.building_dwid, a.address_dwid,  pj.project_dwid , null as lot_group_dwid
from id_base base
left join masterdata_hk.address a on base.dw_address_id = a.dw_address_id 
left join masterdata_hk.building b on base.dw_building_id = b.dw_building_id
left join masterdata_hk.project pj on base.dw_project_id = pj.dw_project_id 
--left join masterdata_hk.property prop on base.dw_property_id = prop.dw_property_id 
where base.seq2 = 1
; -- 19992 --> 17530 after dedup


select *
from map_hk.midland_building_to_dwid
limit 500


select *
from map_hk.midland_unit_to_dwid
limit 500
























'''
how to improve data quality and check consistency:
1.address table , missing phase, use hk data gov address lookup table to match phase `datalake_raw.hk_datagov_address`, may need dedup

2.consistency check

3.midland transaction map also need to join address and street 
-- need 2 kinds of map:
one is for uuid and ids map -- help to get the link / relationship for current main table and their source data, need to update when merge change request; 
another is for features and ids map -- help to find the ids when we have new records want to merge into main table, also need to update when we didnt match any ids for new records;

4.building table need to update completion year feature based on different phase 
-- need to check the consistency with phase info in address table, 
-- make sure we could link back phase info and implement different completion year for buildings in different phase
-- Have some problems on ids consistency among address / project / building table, eg: missing address_dwid


'''


CREATE TABLE raw_hongkong.hk_datagov_address (
	source_file varchar NULL,
	eng_buildingname varchar NULL,
	engblock_blockdescriptor varchar NULL,
	engblock_buildingno varchar NULL,
	engblock_blockdescriptorprecedenceindicator varchar NULL,
	engestate_estatename varchar NULL,
	engphase_phasename varchar NULL,
	engphase_phaseno varchar NULL,
	engstreet_locationname varchar NULL,
	engstreet_streetname varchar NULL,
	engstreet_buildingnofrom varchar NULL,
	engstreet_buildingnoto varchar NULL,
	engdistrict_dcdistrict varchar NULL,
	eng_region varchar NULL,
	chi_buildingname varchar NULL,
	chiblock_blockdescriptor varchar NULL,
	chiblock_buildingno varchar NULL,
	chiblock_blockdescriptorprecedenceindicator varchar NULL,
	chiestate_estatename varchar NULL,
	chiphase_phasename varchar NULL,
	chiphase_phaseno varchar NULL,
	chistreet_locationname varchar NULL,
	chistreet_streetname varchar NULL,
	chistreet_buildingnofrom varchar NULL,
	chistreet_buildingnoto varchar NULL,
	chidistrict_dcdistrict varchar NULL,
	chi_region varchar NULL,
	geoaddress varchar NULL,
	geospatialinformation_northing varchar NULL,
	geospatialinformation_easting varchar NULL,
	geospatialinformation_latitude varchar NULL,
	geospatialinformation_longitude varchar NULL,
	address_validationinformation_score varchar NULL,
	requestaddress varchar NULL
);



select engestate_estatename, count(distinct engphase_phasename) as phase_count
from raw_hongkong.hk_datagov_address
group by 1 having count(distinct engphase_phasename) > 0
order by 2 desc;
''' -- only 75 estates have phase info 
DISCOVERY BAY	19
WHAMPOA GARDEN	10
LOHAS PARK	8
MEI FOO SUN CHUEN	8
TAIKOO SHING	8
KINGSWOOD VILLAS	6
PALM SPRINGS	5
DOUBLE COVE	5
UNION SQUARE	4
SOUTH HORIZONS	4
SEA CREST VILLA	4
BEACON HEIGHTS	4
CARIBBEAN COAST	4
VILLAGE GARDENS	3
THE BLOSSOM	3
KWAN YICK BUILDING	3
HEALTHY VILLAGE	3
GREENWOOD VILLAS	3
CLASSICAL GARDENS	3
BELVEDERE GARDEN	3
WYLER GARDENS	3
FESTIVAL CITY	3
METRO CITY	3
LAGUNA VERDE	3
DYNASTY HEIGHTS	3
THE BLOOMSWAY	3
SHATINPARK	3
JAZZ GARDEN	2
IMPERIAL VILLAS	2
KISLAND VILLA	2
KI TAT GARDEN	2
HANG CHEONG VILLAS	2
SYMPHONY BAY	2
MING YUEN MANSIONS	2
PICTORIAL GARDEN	2
IN KEEN GARDEN	2
SWALLOW GARDEN	2
GARDEN VISTA	2
PO SHU GARDEN	2
GOLDEN COVE LOOKOUT	2
YUK YAT GARDENS	2
ON WO YUEN	2
FU TOR LOY SUN CHUEN	2
VALAIS	2
TAI HING GARDENS	2
CHUN WAH VILLAS	2
GOLDEN LION GARDEN	2
ISLAND SOUTH	1
CHI FU FA YUEN	1
KWUN TONG GARDEN EST	1
LAM TIN ESTATE	1
HONG KONG GARDEN	1
BEL-AIR ON THE PEAK	1
HANG FUK BUILDING	1
TAK CHUNG GARDEN	1
PARK ISLAND	1
SERENO VERDE	1
SERENO VERDE PHASE 5	1
TSZ ON COURT	1
OAK HILL	1
RESIDENCE BEL-AIR	1
SUNSHINE CITY	1
HK JOCKEY CLUB	1
PO TONG GARDEN	1
PO WAH GARDEN	1
PASSERELLE	1
MEADOWLANDS	1
HARBOUR CITY	1
CLASSICAL GDNS	1
FU YEE GARDEN	1
NEW TOWN PLAZA	1
LEON COURT	1
HONG NING ROAD PARK	1
FULLER GARDENS	1
THE VINEYARD	1
'''





CREATE TABLE feature_hk.de__project__tenure_info (
	project_dwid varchar NOT NULL,
	tenure_start_date text NULL,
	CONSTRAINT de__project__tenure_info_pk PRIMARY KEY (project_dwid),
	CONSTRAINT de__project__tenure_info_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid)
);


insert into feature_hk.de__project__tenure_info
(
	project_dwid,  tenure_start_date
)
with base as (
select project_dwid , attribute_value as tenure_start_date, row_number() over(partition by project_dwid order by attribute_value) as seq
from masterdata_hk.project_attribute pa 
where attribute_name = 'tenure_start_date'
)
select project_dwid, tenure_start_date
from base where seq = 1
;


select project_dwid , count(*)
from masterdata_hk.project_attribute pa 
where attribute_name = 'tenure_start_date'
group by 1 having count(*) > 1


select *
from masterdata_hk.project_attribute pa 
where attribute_name = 'tenure_start_date'
and project_dwid = '89f663cd63d7408776b064305ffb0597'



----------------------------------------------



CREATE TABLE feature_hk.de__building__completion_year_info (
	building_dwid varchar NOT NULL,
	construction_start text NULL,
	construction_end text NULL,
	completion_year text NULL,
	CONSTRAINT de__building__completion_year_info_pk PRIMARY KEY (building_dwid),
	CONSTRAINT de__building__completion_year_info_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid)
);


select estate_name, phase_name, building_name, building_id, building_first_op_date as completion_date, row_number() over(partition by building_id order by building_first_op_date) as seq
from source.hk_midland_realty_sale_transaction hmrst 
group by 1, 2, 3, 4, 5


with completion_year_base as (
	select building_id, building_first_op_date as completion_date, row_number() over(partition by building_id order by building_first_op_date) as seq
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
select p.building_dwid , cy.completion_date as tenure_start_date
, b.building_name , b.development , b.development_phase 
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_base cy on mid.building_id = cy.building_id and cy.seq = 1
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
where p.project_dwid notnull and cy.completion_date notnull
group by 1,2 --8,590
,3, 4, 5 order by 4, 5, 3



---------

select b.building_dwid , b.address_display_text , a.address_dwid --, b.original_slug 
from masterdata_hk.building b 
left join masterdata_hk.address a on b.address_display_text = a.full_address_text 
where b.address_dwid isnull and a.address_type_code = 'building-address'


-- '5 seabee lane,,,,lantau island - discovery bay,islands,new territories,hong kong (sar)'

-- '5 seabee lane,,,,lantau island - discovery bay,islands,new territories,hong kong (sar)'

with address_base as (
	select b.building_dwid , b.address_display_text , a.address_dwid --, b.original_slug 
	from masterdata_hk.building b 
	left join masterdata_hk.address a on b.address_display_text = a.full_address_text 
	where b.address_dwid isnull and a.address_type_code = 'building-address'
)
update masterdata_hk.building b 
set address_dwid = 
(select a.address_dwid from address_base a where a.building_dwid = b.building_dwid )
where b.address_dwid isnull
; -- 170



-- '12 section k 5th street,house 12 5th street,section k,fairview park,yuen long,yuen long,new territories,hong kong (sar)'

-- '12 section k 5th street,house 12 5th street,section k,fairview park,yuen long,yuen long,new territories,hong kong (sar)'


with address_base as (
	select b.building_dwid , b.address_display_text , a.address_dwid --, b.original_slug 
	from masterdata_hk.building b 
	left join masterdata_hk.address a on b.address_display_text = a.full_address_text 
	where b.address_dwid isnull --and a.address_type_code = 'building-address'
)
update masterdata_hk.building b 
set address_dwid = 
(select a.address_dwid from address_base a where a.building_dwid = b.building_dwid )
where b.address_dwid isnull
;

with base as (
select 
	b.building_dwid , b.development as original_development, b.development_phase as original_phase,
	a.development as new_development, a.development_phase as new_phase
from masterdata_hk.building b
left join masterdata_hk.address a using (address_dwid)
)
select 
	case when new_development isnull and original_development notnull then 1 else 0 end as missing_new_development,
	case when new_phase isnull and original_phase notnull then 1 else 0 end as missing_new_phase,
	count(*)
from base 
group by 1, 2
; -- 0	0	63370 -- no MISSING development and phase info if we use address_dwid to join development and development_phase


update masterdata_hk.building b
set (development, development_phase) = 
(select a.development, a.development_phase
from masterdata_hk.building bb
left join masterdata_hk.address a using (address_dwid)
where b.building_dwid = bb.building_dwid)
where b.building_dwid notnull
;


with completion_year_base as (
	select building_id, building_first_op_date as completion_date, row_number() over(partition by building_id order by building_first_op_date) as seq
	from source.hk_midland_realty_sale_transaction hmrst 
	group by 1, 2
)
, building_completion_year as (
select p.building_dwid , date_part('year', cy.completion_date::date) as construction_end_year
, b.building_name , b.development , b.development_phase , b.project_dwid , row_number() over(partition by p.building_dwid order by cy.completion_date desc) as seq -- use the latest one 
from masterdata_hk.dm_property_activity dpa
left join masterdata_hk.property p using (dw_property_id)
left join source.hk_midland_realty_sale_transaction mid on dpa.activity_source_id = mid.data_uuid::text
left join completion_year_base cy on mid.building_id = cy.building_id and cy.seq = 1
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
where p.project_dwid notnull and cy.completion_date notnull
group by 1, 2, 3, 4, 5, 6, cy.completion_date order by 4, 5, 3
)
, building_completion_year_final as (
select building_dwid, construction_end_year
from building_completion_year
where seq = 1
)
update masterdata_hk.building b
set construction_end_year = 
(select bc.construction_end_year from building_completion_year_final bc
where b.building_dwid = bc.building_dwid and bc.construction_end_year notnull)
where b.building_dwid notnull;


with missing_building_completion_year as (
select b.building_dwid , date_part('year', dpti.tenure_start_date ::date) as construction_end_year
from masterdata_hk.building b
left join masterdata_hk.project p using (project_dwid)
left join feature_hk.de__project__tenure_info dpti on p.project_dwid = dpti.project_dwid 
where b.construction_end_year isnull and dpti.tenure_start_date notnull
) -- if midland does not have specific completion year for this building then use project level tenure_start_date as it's builidng completion year
update masterdata_hk.building b
set construction_end_year = 
(select c.construction_end_year from missing_building_completion_year c where b.building_dwid = c.building_dwid)
where b.construction_end_year isnull
; -- 56372




insert into feature_hk.de__building__completion_year_info 
(
	building_dwid, construction_end, completion_year
)
select building_dwid, construction_end_year, construction_end_year
from masterdata_hk.building
where construction_end_year notnull




----------------------------------------------

-- integrate new launch data 


CREATE TABLE masterdata_hk.dm_project_new_launch (
	dw_project_id text NULL,
	project_name text NULL,
	project_display_name text NULL,
	project_display_name_cn text NULL,
	project_display_name_hk text NULL,
	project_display_name_my text NULL,
	project_alias text NULL,
	property_type_id float4 NULL,
	project_property_types text NULL,
	real_estate_type text NULL,
	address_display_text text NULL,
	address_display_text_cn text NULL,
	address_display_text_hk text NULL,
	address_display_text_my text NULL,
	location_display_text text NULL,
	ownership_type text NULL,
	tenure text NULL,
	tenure_type text NULL,
	tenure_expiry_text text NULL,
	tenure_start_date text NULL,
	tenure_end_date text NULL,
	project_status text NULL,
	launch_date text NULL,
	construction_status text NULL,
	construction_start_year text NULL,
	expected_occupancy_date date NULL,
	occupancy_date text NULL,
	legal_completion_date text NULL,
	completion_year float4 NULL,
	decommission_year float4 NULL,
	region_state_province text NULL,
	location_marker text NULL,
	latitude float4 NULL,
	longitude float4 NULL,
	facilities_text text NULL,
	special_attributes text NULL,
	is_strata_titled_issued text NULL,
	min_gross_floor_area_sqm float4 NULL,
	max_gross_floor_area_sqm float4 NULL,
	min_gross_floor_area_sqft float4 NULL,
	max_gross_floor_area_sqft float4 NULL,
	land_size_sqm float4 NULL,
	project_cost_class text NULL,
	maintenance_fee_basis text NULL,
	maintenance_fee_unit_price float4 NULL,
	project_description text NULL,
	tag_text text NULL,
	parking_text text NULL,
	restrictions_text text NULL,
	project_url text NULL,
	slug text NULL,
	data_source text NULL,
	data_source_key text NULL,
	data_source_additional_id text NULL,
	data_source_count int8 NULL,
	is_active boolean NULL,
	country_code text NULL,
	currency_code text NULL,
	developer_name text NULL,
	dw_developer_id text NULL,
	last_30_days_sale_transaction_count float4 NULL,
	last_60_days_sale_transaction_count float4 NULL,
	last_30_days_sale_listing_count float4 NULL,
	last_60_days_sale_listing_count float4 NULL,
	last_30_days_rent_transaction_count float4 NULL,
	last_60_days_rent_transaction_count float4 NULL
);


























