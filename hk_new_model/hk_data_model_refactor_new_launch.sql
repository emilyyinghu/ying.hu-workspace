----------------------------------------------

-- integrate new launch data 

select f_clone_table('masterdata_hk', 'address', 'masterdata_hk', 'address_backup', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'building', 'masterdata_hk', 'building_backup', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'project', 'masterdata_hk', 'project_backup', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'property', 'masterdata_hk', 'property_backup', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'sale_transaction', 'masterdata_hk', 'sale_transaction_backup', TRUE, TRUE);





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





select *
from masterdata_hk.dm_address_new_launch danl 
limit 100


select *
from masterdata_hk.dm_project_new_launch danl 
limit 100


select a.dw_address_id , a.full_address_text , p.dw_project_id , p.project_name 
from masterdata_hk.dm_address_new_launch a 
left join masterdata_hk.dm_project_new_launch p on lower(a.address_building) = lower(p.project_name)


-- create table masterdata_hk.address_new_launch (like masterdata_hk.address)

select *
from reference.hk_new_launch_meta
limit 100

select *
from reference.hk_new_launch_schematic
limit 100


select *
from masterdata_hk.address a 
where address_type_code = 'point-address'
limit 100

select *
from masterdata_hk.address a 
where address_type_code = 'building-address'
limit 100


select project_name,
	case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
		when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
		then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
		else meta.project_name
		end as cleaned_project_name,
	case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
		when meta.project_name ilike '% iii%' then 'iii'
		when meta.project_name ilike '% ii%' then 'ii'
		when meta.project_name ilike '% i%' then 'i'
		end as phase
from reference.hk_new_launch_meta meta
group by 1,2,3 order by 1;


---------------
-- 1.address table 
-- point address
select 
	meta.address_street ,
	meta.address_num ,
	case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
		when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
		then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
		else meta.project_name
		end as project_name,
	case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
		when meta.project_name ilike '% iii%' then 'iii'
		when meta.project_name ilike '% ii%' then 'ii'
		when meta.project_name ilike '% i%' then 'i'
		end as phase,
	sch.block,
	initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
	--md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_project_id,
	md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_address_id
from reference.hk_new_launch_schematic sch
left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
-- group by 
;

-- trim(trim(coalesce(address_street_text,'')||','||coalesce(address_building,'')||','||coalesce(development_phase,'')||','||
-- 	    coalesce(development,'')||','||coalesce(city_subarea,'')||','||coalesce(city_area,'')||','||city||','||metro_area, ',')) else full_address_text end as full_address_text,

with point_address_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else meta.project_name
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num
		--md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_project_id,
		--md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_address_id
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	group by 1,2,3,4,5,6
)
select 
	--id,
	--address_dwid,
	'point-address' as address_type_code,
	null as address_type_attribute,
	lower(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(block,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as full_address_text,
	initcap(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(block,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as address_display_text,
	block as address_building,
	address_num as address_number,
	--address_number_range,
	--address_number_range_start,
	--address_number_range_end,
	trim(coalesce(address_num,'')||' '||coalesce(address_street,'')) as address_street_text,
	--street_type,
	--street_prefix,
	--street_prefix_type,
	--street_side,
	address_street as street_name,
	address_street as street_name_root,
	--street_suffix,
	--postal_code_ext,
	--postal_code,
	project_name as development,
	--development_code,
	phase as development_phase,
	--development_phase_number,
	--neighborhood_block, neighborhood_block_code, neighborhood_section, neighborhood, neighborhood_code,neighborhood_group,
	--city_subarea,city_subarea_id,city_subarea_code,city_district,city_area,city_area_id,city_area_code,city,city_id,city_code,
	'hong kong (sar)' as metro_area,
	'hk' as metro_area_code,
	--metro_area_district,region_admin_subdistrict,region_admin_subdistrict_code,region_admin_district,region_admin_district_id,
	--region_admin_district_code,region_state_province,region_state_province_code,region_state_province_alias,territory,territory_code,
	'china' as country,
	'cn' as country_code,
	'chn' as country_3code,
	'south-east asia' as geographic_zone,
	'asia' as continent,
	--latitude,longitude,location_marker,
	trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'/'||coalesce(phase,'')||'/'||
 	    coalesce(block,'')||'/'||coalesce(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')),'') 
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/')) as slug,
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'/'||coalesce(phase,'')||'/'||
 	    coalesce(block,'')||'/'||coalesce(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')),'') 
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) as dw_address_id
from point_address_base
; --204


-- building address
-- unnecessary because they are all point-address, no single buidling-address

-- project address 
with project_address_base as 
(
	select 
		meta.address_street ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else meta.project_name
			end as project_name
		--md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_project_id,
		--md5(lower('hk__'|| meta.address_street || '__' || meta.project_name)) AS dw_address_id
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	group by 1,2
)
select 
	--id,
	--address_dwid,
	'project-address' as address_type_code,
	null as address_type_attribute,
	lower(trim(trim(coalesce(address_street,'')||',,,'||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as full_address_text,
	initcap(trim(trim(coalesce(address_street,'')||',,,'||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as address_display_text,
	null as address_building,
	null as address_number,
	--address_number_range,
	--address_number_range_start,
	--address_number_range_end,
	address_street as address_street_text,
	--street_type,
	--street_prefix,
	--street_prefix_type,
	--street_side,
	address_street as street_name,
	address_street as street_name_root,
	--street_suffix,
	--postal_code_ext,
	--postal_code,
	project_name as development,
	--development_code,
	null as development_phase,
	--development_phase_number,
	--neighborhood_block, neighborhood_block_code, neighborhood_section, neighborhood, neighborhood_code,neighborhood_group,
	--city_subarea,city_subarea_id,city_subarea_code,city_district,city_area,city_area_id,city_area_code,city,city_id,city_code,
	'hong kong (sar)' as metro_area,
	'hk' as metro_area_code,
	--metro_area_district,region_admin_subdistrict,region_admin_subdistrict_code,region_admin_district,region_admin_district_id,
	--region_admin_district_code,region_state_province,region_state_province_code,region_state_province_alias,territory,territory_code,
	'china' as country,
	'cn' as country_code,
	'chn' as country_3code,
	'south-east asia' as geographic_zone,
	'asia' as continent,
	--latitude,longitude,location_marker,
	trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'///'||coalesce(address_street,'')
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/')) as slug,
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'///'||coalesce(address_street,'')
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) AS dw_address_id
from project_address_base
; --48


-- UNION point and project level address and insert them into masterdata_hk.address - how to flag them with 'new-launch' identifier?


insert into masterdata_hk.address -- 252 new records for new launch -- their full_address_text only have metro_area without city/city_area/city_subarea!
(
	address_type_code,address_type_attribute,full_address_text,address_display_text,address_building,address_number,
	address_street_text,street_name,street_name_root,
	development,development_phase,metro_area,metro_area_code,country,country_code,country_3code,geographic_zone,
	continent,slug,status_code,language_code,data_source_count,hash_key,dw_address_id
)
(with point_address_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else meta.project_name
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	group by 1,2,3,4,5,6
)
select 
	--id,
	--address_dwid,
	'point-address' as address_type_code,
	null as address_type_attribute,
	lower(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(block,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as full_address_text,
	initcap(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(block,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as address_display_text,
	block as address_building,
	address_num as address_number,
	trim(coalesce(address_num,'')||' '||coalesce(address_street,'')) as address_street_text,
	address_street as street_name,
	address_street as street_name_root,
	project_name as development,
	phase as development_phase,
	'hong kong (sar)' as metro_area,
	'hk' as metro_area_code,
	'china' as country,
	'cn' as country_code,
	'chn' as country_3code,
	'south-east asia' as geographic_zone,
	'asia' as continent,
	--latitude,longitude,location_marker,
	trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'/'||coalesce(phase,'')||'/'||
 	    coalesce(block,'')||'/'||coalesce(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')),'') 
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/')) as slug,
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'/'||coalesce(phase,'')||'/'||
 	    coalesce(block,'')||'/'||coalesce(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')),'') 
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) as dw_address_id
from point_address_base
)
UNION
(with project_address_base as 
(
	select 
		meta.address_street ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else meta.project_name
			end as project_name
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	group by 1,2
)
select 
	--id,
	--address_dwid,
	'project-address' as address_type_code,
	null as address_type_attribute,
	lower(trim(trim(coalesce(address_street,'')||',,,'||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as full_address_text,
	initcap(trim(trim(coalesce(address_street,'')||',,,'||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as address_display_text,
	null as address_building,
	null as address_number,
	address_street as address_street_text,
	address_street as street_name,
	address_street as street_name_root,
	project_name as development,
	--development_code,
	null as development_phase,
	'hong kong (sar)' as metro_area,
	'hk' as metro_area_code,
	'china' as country,
	'cn' as country_code,
	'chn' as country_3code,
	'south-east asia' as geographic_zone,
	'asia' as continent,
	trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'///'||coalesce(address_street,'')
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/')) as slug,
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'///'||coalesce(address_street,'')
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) AS dw_address_id
from project_address_base
);


UPDATE masterdata_hk.address 
SET address_dwid = md5(country_code||'__'||'address'||'__'||id) 
WHERE address_dwid isnull ;



-- 2.project table
select * from masterdata_hk.project p limit 100;
select * from masterdata_hk.dm_project_new_launch p limit 100;

select * from masterdata_hk.address a 
where address_type_code = 'project-address' 
and city isnull;



insert into masterdata_hk.project -- insert 48 new records, their slug start with 'hk/' rather than 'cn/hk/'
(
	address_dwid,project_type_code,project_display_name,project_name_text,completion_year,tenure_code,
	unit_count,residential_unit_count,commercial_unit_count,address_display_text,location_marker,
	country_code,is_active,dw_address_id,dw_project_id,original_slug
)
with point_address_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else replace(replace(meta.project_name,'’s','''s'), '．',' ')
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
		case when ppt.property_type_id notnull then ppt.property_type_id
			when uip.propertygroup = 'Non-Landed' then '120600000'
			when uip.propertygroup = 'Landed' then '110000000'
			else '120600000'
			end as property_type_id,
		md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) as original_dw_project_id
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
		left join reference.idx_project_property_type ppt on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = ppt.dw_project_id 
		left join "source".user_input_dmproject_uniqueness uip on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = uip.dwprojectid 
	group by 1,2,3,4,5,6,7,8
)
, dedup as (
select 
	--id,
	--project_dwid,
	a.address_dwid as address_dwid,
	case when base.property_type_id = '120600000' then 'condo'
		when base.property_type_id = '120900010' then 'hos'
		when base.property_type_id = '200000000' then 'commercial'
		when base.property_type_id::text ilike '110%' then 'condo-w-house' -- HK may not have Landed units, but we may identify them as 'HOUSE'
	end as project_type_code,
	--project_name,
	initcap(base.project_name) as project_display_name,
	lower(base.project_name) as project_name_text,
	EXTRACT('year' FROM meta.completion_year::date)::int as completion_year,
	'leasehold' as tenure_code,
	meta.total_units as unit_count,
	meta.total_units as residential_unit_count,
	null::int as commercial_unit_count,
	a.address_display_text,
	st_astext(st_makepoint(p.longitude, p.latitude)) as location_marker,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	original_dw_project_id as dw_address_id ,
	original_dw_project_id as dw_project_id ,
	p.slug as original_slug,
	row_number() over(partition by base.project_name order by p.longitude, p.latitude) as seq
from point_address_base base
left join masterdata_hk.address a on lower(base.project_name) = lower(a.development) and lower(base.address_street) = lower(a.street_name) and a.address_type_code = 'project-address' and a.city isnull 
left join reference.hk_new_launch_meta meta on base.original_dw_project_id = md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' ')))
left join masterdata_hk.dm_project_new_launch p on base.original_dw_project_id = p.dw_project_id 
)
select 
	address_dwid,project_type_code,project_display_name,project_name_text,completion_year,tenure_code,
	unit_count,residential_unit_count,commercial_unit_count,address_display_text,location_marker,
	country_code,is_active,dw_address_id,dw_project_id,original_slug
from dedup
where seq = 1;


select md5(lower('hk__'|| 'plunkett''s road' || '__' || '3 plunkett''s road'));


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
 

-- 3.building table

select * from masterdata_hk.building b limit 100;
select * from masterdata_hk.dm_building_new_launch b limit 100;

select * from masterdata_hk.address a 
where city isnull;



insert into masterdata_hk.building  -- insert 184 + 19 new records, their slug start with 'hk/' rather than 'cn/hk/'
(
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_display_name,building_name_text,
	construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	country_code,is_active,development,development_phase,dw_address_id,dw_building_id,dw_project_id,original_slug
)


with point_address_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else replace(replace(meta.project_name,'’s','''s'), '．',' ')
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
		case when ppt.property_type_id notnull then ppt.property_type_id
			when uip.propertygroup = 'Non-Landed' then '120600000'
			when uip.propertygroup = 'Landed' then '110000000'
			else '120600000'
			end as property_type_id,
		md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) as original_dw_project_id,
		count(distinct sch.id) as unit_count
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
		left join reference.idx_project_property_type ppt on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = ppt.dw_project_id 
		left join "source".user_input_dmproject_uniqueness uip on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = uip.dwprojectid 
	group by 1,2,3,4,5,6,7,8
)
, dedup1 as (
select distinct 
	--id,
	--building_dwid,
	a.address_dwid as address_dwid,
	p.project_dwid as project_dwid,
	null as lot_group_dwid,
	-- building_name,
	base.block_num as building_block_number,
	initcap(base.block_num) as building_display_name,
	lower(base.block_num) as building_name_text,
	p.completion_year as construction_end_year, 
	base.unit_count as unit_count,
	base.unit_count as residential_unit_count,
	null::int as commercial_unit_count,
	lower(a.address_display_text) as address_display_text,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	-- columns below are only for testing
	a.development,
	a.development_phase,
	a.dw_address_id,
	b.dw_building_id,
	p.dw_project_id,
	b.slug as original_slug,
	row_number() over(partition by a.dw_address_id, base.block_num order by p.completion_year) as seq
from point_address_base base
left join masterdata_hk.address a on lower(base.project_name) = lower(a.development) and lower(base.block_num) = lower(coalesce(a.address_building, a.address_number)) and lower(base.address_street) = lower(a.street_name) and lower(base.address_num) = lower(a.address_number)
left join masterdata_hk.project p on lower(base.project_name) = lower(p.project_name_text)
left join masterdata_hk.dm_building_new_launch b on base.original_dw_project_id = b.dw_project_id and lower(base.block_num) = lower(b.block_num)
--order by a.development, a.address_display_text
)
, dedup2 as (
select *, row_number() over(partition by development, development_phase, building_display_name order by address_display_text desc) as seq2
from dedup1
where seq = 1
)
select 
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_display_name,building_name_text,
	construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	country_code,is_active,development,development_phase,dw_address_id,dw_building_id,dw_project_id,original_slug
from dedup2
where seq2 = 1 and address_dwid notnull; -- 184



insert into masterdata_hk.building  -- insert 19 new records, their slug start with 'hk/' rather than 'cn/hk/'
(
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_display_name,building_name_text,
	construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	country_code,is_active,development,development_phase,dw_address_id,dw_building_id,dw_project_id,original_slug
)
with point_address_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else replace(replace(meta.project_name,'’s','''s'), '．',' ')
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
		case when ppt.property_type_id notnull then ppt.property_type_id
			when uip.propertygroup = 'Non-Landed' then '120600000'
			when uip.propertygroup = 'Landed' then '110000000'
			else '120600000'
			end as property_type_id,
		md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) as original_dw_project_id,
		count(distinct sch.id) as unit_count
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
		left join reference.idx_project_property_type ppt on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = ppt.dw_project_id 
		left join "source".user_input_dmproject_uniqueness uip on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = uip.dwprojectid 
	where sch.block isnull
		group by 1,2,3,4,5,6,7,8
)
, dedup1 as (
select distinct 
	--id,
	--building_dwid,
	a.address_dwid as address_dwid,
	p.project_dwid as project_dwid,
	null as lot_group_dwid,
	-- building_name,
	base.block_num as building_block_number,
	initcap(base.block_num) as building_display_name,
	lower(base.block_num) as building_name_text,
	p.completion_year as construction_end_year, 
	base.unit_count as unit_count,
	base.unit_count as residential_unit_count,
	null::int as commercial_unit_count,
	lower(a.address_display_text) as address_display_text,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	-- columns below are only for testing
	a.development,
	a.development_phase,
	a.dw_address_id,
	b.dw_building_id,
	p.dw_project_id,
	b.slug as original_slug,
	row_number() over(partition by a.dw_address_id, base.block_num order by p.completion_year) as seq
from point_address_base base
left join masterdata_hk.address a on lower(base.project_name) = lower(a.development) and lower(base.block_num) = lower(coalesce(a.address_building, a.address_number)) and lower(base.address_street) = lower(a.street_name) and lower(base.address_num) = lower(a.address_number)
left join masterdata_hk.project p on lower(base.project_name) = lower(p.project_name_text)
left join masterdata_hk.dm_building_new_launch b on base.original_dw_project_id = b.dw_project_id and lower(base.block_num) = lower(b.block_num)
--order by a.development, a.address_display_text
)
, dedup2 as (
select *, row_number() over(partition by development, development_phase, building_display_name order by address_display_text desc) as seq2
from dedup1
where seq = 1
)
select 
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_display_name,building_name_text,
	construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	country_code,is_active,development,development_phase,dw_address_id,dw_building_id,dw_project_id,original_slug
from dedup2
where seq2 = 1 and address_dwid notnull;



UPDATE masterdata_hk.building 
SET building_name = api.get_normalized_name(building_name_text) 
WHERE building_name isnull ;

UPDATE masterdata_hk.building 
SET building_dwid = md5(country_code||'__'||'building'||'__'||id) WHERE building_dwid isnull ;

UPDATE masterdata_hk.building 
SET slug = api.clean_slug(country_code||'/'||address_dwid||'/'||coalesce(building_name,'')||'-'||id) 
WHERE slug isnull ;
 

select *
from masterdata_hk.building 
where original_slug  ilike 'hk/%'

-- delete from masterdata_hk.building where original_slug ilike 'hk/%';


-- too slow
-- DELETE FROM masterdata_hk.project WHERE id = 3059512;
-- DELETE FROM masterdata_hk.address WHERE id = 645735838;
-- DELETE FROM masterdata_hk.address WHERE id >= 645735794 and id <= 645735814;

('645735794', '645735795', '645735796', '645735797', '645735798', '645735799', '645735800', '645735801', '645735802', '645735803', '645735804', '', '', '')

645735805
645735806
645735807
645735808
645735809
645735810
645735811
645735812
645735813
645735814
645735838


select *
from masterdata_hk.address WHERE id >= 645735794 and id <= 645735814;

select *
from masterdata_hk.project
where address_dwid in (
select address_dwid from masterdata_hk.address WHERE id = 645735838
)


-- 4.property table

select * from masterdata_hk.property p limit 100;
select * from masterdata_hk.dm_property_new_launch p limit 100;

select * from masterdata_hk.address a 
where city isnull;


insert into masterdata_hk.property -- insert 18440 new records
(
	address_dwid,building_dwid,project_dwid,property_type_code,address_unit,address_floor_text,address_floor_num,address_stack,
	bedroom_count,bathroom_count,net_floor_area_sqm,gross_floor_area_sqm,country_code,is_active,
	dw_property_id,dw_address_id,dw_building_id,dw_project_id,original_slug
)
with unit_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else replace(replace(meta.project_name,'’s','''s'), '．',' ')
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
		case when ppt.property_type_id notnull then ppt.property_type_id
			when uip.propertygroup = 'Non-Landed' then '120600000'
			when uip.propertygroup = 'Landed' then '110000000'
			else '120600000'
			end as property_type_id,
		md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) as original_dw_project_id,
		replace(sch.floor, '.0', '') as floor,
		case when replace(sch.floor, '.0', '') ilike '%g%' or replace(sch.floor, '.0', '') like '%房%' or replace(sch.floor, '.0', '') ilike '%house%' then 1--::text
            else replace(sch.floor, '.0', '')::int
        	end as floor_num,
		lower(sch.stack) as stack,
		sch.built_up_area_sqm as gross_floor_area_sqm,
        sch.built_up_area_sqft as gross_floor_area_sqft,
        sch.floor_area_sqm as net_floor_area_sqm,
        sch.floor_area_sqft as net_floor_area_sqft,
        sch.num_of_bedrooms ,
        sch.num_of_bathrooms 
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
		left join reference.idx_project_property_type ppt on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = ppt.dw_project_id 
		left join "source".user_input_dmproject_uniqueness uip on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = uip.dwprojectid 
)
select 
	--id,
	--property_dwid,
	a.address_dwid,
	b.building_dwid,
	b.project_dwid,
	case when base.property_type_id = '120600000' then 'condo'
		when base.property_type_id = '120900010' then 'hos'
		when base.property_type_id = '200000000' then 'comm'
	end as property_type_code,
	base.floor||'-'||base.stack as address_unit,
	base.floor as address_floor_text,
	base.floor_num as address_floor_num,
	base.stack as address_stack,
	base.num_of_bedrooms as bedroom_count,
	base.num_of_bathrooms as bathroom_count,
	base.net_floor_area_sqm,
	base.gross_floor_area_sqm,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	-- for test
	--a.development ,
	--a.development_phase ,
	--a.address_building ,
	--
	prop.dw_property_id,
	a.dw_address_id,
	b.dw_building_id,
	p.dw_project_id,
	prop.slug as original_slug
from unit_base base
left join masterdata_hk.address a on base.original_dw_project_id = a.dw_address_id and lower(base.block) = lower(a.address_building) and a.address_type_code = 'point-address'
left join masterdata_hk.building b on base.original_dw_project_id = b.dw_project_id and lower(base.block_num) = lower(b.building_block_number)
left join masterdata_hk.project p on base.original_dw_project_id = p.dw_project_id
left join masterdata_hk.dm_property_new_launch prop on base.original_dw_project_id = prop.dw_project_id 
	and lower(base.block_num) = lower(prop.building_name)
	and lower(base.floor) = lower(prop.address_floor) and lower(base.stack) = lower(prop.address_stack)
; --18,440


UPDATE masterdata_hk.property 
SET property_dwid = md5(country_code||'__'||'property'||'__'||id) 
WHERE property_dwid isnull ;

UPDATE masterdata_hk.property 
SET slug = api.clean_slug(country_code||'/'||property_type_code||'/'||building_dwid||'/'||address_unit||'-'||id) 
WHERE slug isnull ;
 


-- 5.sale transaction table
select * from masterdata_hk.sale_transaction_new st limit 100;
select * from masterdata_hk.dm_property_activity_new_launch st where activity_type = 'sale_transaction' limit 100;

select * from masterdata_hk.address a 
where city isnull;



with unit_base as 
(
	select 
		meta.address_street ,
		meta.address_num ,
		case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
			when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
			then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
			else replace(replace(meta.project_name,'’s','''s'), '．',' ')
			end as project_name,
		case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
			when meta.project_name ilike '% iii%' then 'iii'
			when meta.project_name ilike '% ii%' then 'ii'
			when meta.project_name ilike '% i%' then 'i'
			end as phase,
		sch.block,
		initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
		case when ppt.property_type_id notnull then ppt.property_type_id
			when uip.propertygroup = 'Non-Landed' then '120600000'
			when uip.propertygroup = 'Landed' then '110000000'
			else '120600000'
			end as property_type_id,
		md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) as original_dw_project_id,
		replace(sch.floor, '.0', '') as floor,
		case when replace(sch.floor, '.0', '') ilike '%g%' or replace(sch.floor, '.0', '') like '%房%' or replace(sch.floor, '.0', '') ilike '%house%' then 1--::text
            else replace(sch.floor, '.0', '')::int
        	end as floor_num,
		lower(sch.stack) as stack,
		sch.built_up_area_sqm as gross_floor_area_sqm,
        sch.built_up_area_sqft as gross_floor_area_sqft,
        sch.floor_area_sqm as net_floor_area_sqm,
        sch.floor_area_sqft as net_floor_area_sqft,
        sch.num_of_bedrooms ,
        sch.num_of_bathrooms 
	from reference.hk_new_launch_schematic sch
		left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
		left join reference.idx_project_property_type ppt on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = ppt.dw_project_id 
		left join "source".user_input_dmproject_uniqueness uip on md5(lower('hk__'|| meta.address_street || '__' || replace(replace(meta.project_name,'’s','''s'), '．',' '))) = uip.dwprojectid 
)
, 

with txn_nl as (
	select st.*, p.*
	from masterdata_hk.dm_property_activity_new_launch st 
	left join masterdata_hk.dm_property_new_launch p using (dw_property_id)
	where activity_type = 'sale_transaction'
)



-- not useful now because those new launch properties in masterdata_hk.property table their dw_property_id are NULL
select 
	--id,
	--activity_dwid,
	prop.property_dwid , 
	a.address_dwid , 
	b.building_dwid , 
	p.project_dwid , 
	-- null as lot_group_dwid,
	-- null as current_lot_group_dwid,
	-- null as activity_name,
	1::int as units_sold,
	case when hmrst.mkt_type = 2 then 'resale'
		--when hmrst.mkt_type = 1 then 'new-sale'
		else 'new-sale' end as sale_type,
	-- null as sale_subtype,	
	b.construction_end_year as property_completion_year,
	prop.property_type_code as property_type_code,
	prop.address_unit as address_unit,
	a.full_address_text as address_local_text,
	p.tenure_code as tenure_code,
	prop.bathroom_count as bathroom_count,
	prop.bedroom_count as bedroom_count,
	prop.gross_floor_area_sqm as gross_floor_area_sqm,
	prop.net_floor_area_sqm as net_floor_area_sqm,
	-- null as land_area_sqm,
	-- null as contract_date,
	st.activity_date as settlement_date, -- update_date
	st.activity_amount as purchase_amount,
	'cn' as country_code,
	hdt.data_uuid::uuid as data_uuid,
	-- null as data_source_uuid,
	st.data_source as data_source,
	st.dw_property_id as dw_property_id ,
	st.slug as original_slug
from masterdata_hk.dm_property_activity_new_launch st 
left join masterdata_hk.property prop on prop.dw_property_id = st.dw_property_id 
left join masterdata_hk.address a on prop.dw_address_id = a.dw_address_id
left join masterdata_hk.building b on prop.dw_building_id = b.dw_building_id 
left join masterdata_hk.project p on prop.dw_project_id = p.dw_project_id 
left join source.hk_daily_transaction hdt on st.activity_date = hdt.instrument_date and hdt.salelable_floor_area*hdt.net_psf::int = st.activity_amount and st.data_source = 'hk-daily-transaction'
left join source.hk_midland_realty_sale_transaction hmrst on hdt.mem_no::text = split_part(hmrst.id, left(hmrst.id, 4),2)
where st.activity_type = 'sale_transaction';




--  another way to get the sale transaction table using actual entities rather than ids 
-- should have 5823 records
with nl_sale_txn_base as (
	select 
		activity_date , activity_amount , st.data_source , st.slug as original_slug, dw_activity_id , 
		st.dw_property_id ,p.dw_building_id, p.dw_address_id, p.dw_project_id, 
		p.address_floor, p.address_stack, p.address_unit, 
		p.bathroom_count, p.bedroom_count, p.gross_floor_area_sqm, p.net_floor_area_sqm, 
		a.full_address_text , b.construction_end_year, pj.tenure_code , prop.property_type_code,
		a.address_dwid , pj.project_dwid , b.building_dwid , prop.property_dwid 
	from masterdata_hk.dm_property_activity_new_launch st 
		left join masterdata_hk.dm_property_new_launch p using (dw_property_id)
		left join masterdata_hk.address a on p.dw_address_id = a.dw_address_id and a.address_type_code = 'point-address' --and lower(p.building_name) = lower(a.address_building)
		left join masterdata_hk.building b on p.dw_building_id = b.dw_building_id  
		left join masterdata_hk.project pj on p.dw_project_id = pj.dw_project_id 
		left join masterdata_hk.property prop on st.dw_property_id = prop.dw_property_id 
		-- on prop.address_dwid  = a.address_dwid and prop.address_floor_text = p.address_floor and prop.address_stack = p.address_stack 
	where st.activity_type = 'sale_transaction'
	-- 5823
)
, nl_sale_txn as (
select 
	--id,
	--activity_dwid,
	base.property_dwid , 
	base.address_dwid , 
	base.building_dwid , 
	base.project_dwid , 
	-- null as lot_group_dwid,
	-- null as current_lot_group_dwid,
	-- null as activity_name,
	1::int as units_sold,
	case when hmrst.mkt_type = 2 then 'resale'
		--when hmrst.mkt_type = 1 then 'new-sale'
		else 'new-sale' end as sale_type,
	-- null as sale_subtype,	
	base.construction_end_year as property_completion_year,
	base.property_type_code as property_type_code,
	base.address_unit as address_unit,
	base.full_address_text as address_local_text,
	base.tenure_code as tenure_code,
	base.bathroom_count as bathroom_count,
	base.bedroom_count as bedroom_count,
	base.gross_floor_area_sqm as gross_floor_area_sqm,
	base.net_floor_area_sqm as net_floor_area_sqm,
	-- null as land_area_sqm,
	-- null as contract_date,
	base.activity_date as settlement_date, -- update_date
	base.activity_amount as purchase_amount,
	'cn' as country_code,
	hdt.data_uuid::uuid as data_uuid,
	-- null as data_source_uuid,
	base.data_source as data_source,
	base.dw_property_id as dw_property_id ,
	base.original_slug as original_slug
	-- column for test:
	, hdt.mem_no
	, hmrst.data_uuid 
	, row_number() over(partition by base.original_slug order by hdt.mem_no, hdt.delivery_date desc, hmrst.data_uuid desc) as seq
from nl_sale_txn_base base 
	left join source.hk_daily_transaction hdt on base.activity_date = hdt.instrument_date 
		and hdt.salelable_floor_area*hdt.net_psf::int = base.activity_amount and base.address_floor = hdt.floor 
		and base.data_source = 'hk-daily-transaction'
	left join source.hk_midland_realty_sale_transaction hmrst on hdt.mem_no::text = split_part(hmrst.id, left(hmrst.id, 4),2)
order by base.original_slug
)-- 7000+ --> 5878
select *
from nl_sale_txn
where seq =1
; -- 5823



-- to do : fill in the dw_property_id for new launch properties in masterdata_hk.property table --> done!
select *
from masterdata_hk.property p 
where dw_property_id isnull; -- 18400 new launch properties

-- round 1
with fix_pnl_base as (
select 
	prop.*, pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
)
select *
from fix_pnl_base
--where dw_property_id_nl isnull -- 9897
where slug_nl isnull --9897



with fix_pnl_base as (
select 
	prop.*, pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
)
, maptable as (
	select id, dw_property_id_nl, slug_nl
	from fix_pnl_base
	where dw_property_id_nl notnull 
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select dw_property_id_nl, slug_nl from maptable m where p.id = m.id)
where p.dw_property_id isnull
; -- fix 8543 missing dw_property_id


select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 9897 new launch properties


-- round 2
with fix_pnl_base as (
select 
	prop.*, pnl.building_name , pnl.block_num , pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
)
select *
from fix_pnl_base prop
where prop.dw_property_id_nl isnull;


select 
	prop.*, 
	pnl.dw_property_id as dw_property_id_nl2, 
	pnl.slug as slug_nl2
from fix_pnl_base prop
left join masterdata_hk.dm_building_new_launch b on prop.dw_building_id = b.dw_building_id 
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and b.block_num = prop.block_num 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id_nl isnull
; -- 11975


with fix_pnl_base as (
select 
	prop.*, pnl.building_name , pnl.block_num , pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
), fix_pnl as (
select 
	prop.*, 
	pnl.dw_property_id as dw_property_id_nl2, 
	pnl.slug as slug_nl2,
	b.building_name , b.building_block_number 
from fix_pnl_base prop
left join masterdata_hk.building b on prop.building_dwid = b.building_dwid  
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and coalesce(lower(b.building_block_number),'')  = coalesce(lower(prop.block_num),'') 
and coalesce(prop.address_floor_text,'') = coalesce(pnl.address_floor,'') and coalesce(prop.address_stack,'') = coalesce(pnl.address_stack,'')
where prop.dw_property_id_nl isnull
)
select *
from fix_pnl prop
where prop.dw_property_id_nl2 isnull;
; -- 8943


-- round 3

with fix_pnl_base as (
select 
	prop.*, pnl.building_name , pnl.block_num , pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
), fix_pnl as (
select 
	prop.*, 
	pnl.dw_property_id as dw_property_id_nl3, 
	pnl.slug as slug_nl3
from fix_pnl_base prop
--left join masterdata_hk.building b on prop.building_dwid = b.building_dwid  
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_project_id = pnl.dw_project_id and prop.dw_building_id = prop.dw_building_id 
and coalesce(prop.address_floor_text,'') = coalesce(pnl.address_floor,'') and coalesce(prop.address_stack,'') = coalesce(pnl.address_stack,'')
where prop.dw_property_id_nl isnull
)
select *
from fix_pnl prop
where prop.dw_property_id_nl3 isnull
; -- 5808


with fix_pnl_base as (
select 
	prop.*, pnl.building_name , pnl.block_num , pnl.dw_property_id as dw_property_id_nl, pnl.slug as slug_nl
from masterdata_hk.property prop
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_address_id = pnl.dw_address_id and prop.dw_building_id = pnl.dw_building_id 
and prop.address_floor_text = pnl.address_floor and prop.address_stack = pnl.address_stack 
where prop.dw_property_id isnull -- 18440
), fix_pnl as (
select 
	prop.*, 
	pnl.dw_property_id as dw_property_id_nl3, 
	pnl.slug as slug_nl3
from fix_pnl_base prop
--left join masterdata_hk.building b on prop.building_dwid = b.building_dwid  
left join masterdata_hk.dm_property_new_launch pnl on prop.dw_project_id = pnl.dw_project_id and prop.dw_building_id = pnl.dw_building_id 
and coalesce(prop.address_floor_text,'') = coalesce(pnl.address_floor,'') and coalesce(prop.address_stack,'') = coalesce(pnl.address_stack,'')
where prop.dw_property_id_nl isnull
)
--select id, count(distinct dw_property_id_nl3) as c
--from fix_pnl
--group by 1 having count(distinct dw_property_id_nl3) > 1
update masterdata_hk.property p
set (dw_property_id, slug) =
(select dw_property_id_nl3, slug_nl3 from fix_pnl m where p.id = m.id)
where p.dw_property_id isnull
; -- fix 4013 missing dw_property_id


select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 5884 new launch properties



-- round 4
select *
from masterdata_hk.property p 
where dw_property_id isnull; -- 5884 new launch properties

--4.1
with base as (
select p.property_dwid , dpnl.dw_property_id 
from masterdata_hk.property p
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.dm_property_new_launch dpnl on dpnl.dw_project_id = p.dw_project_id 
	and dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.address_building) = lower(dpnl.block_num)
where p.dw_property_id isnull
)
select count(*)
from base 
where dw_property_id notnull ; --977


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.dm_property_new_launch dpnl on dpnl.dw_project_id = p.dw_project_id 
	and dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.address_building) = lower(dpnl.block_num)
where p.dw_property_id isnull
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
; -- fix 977 missing dw_property_id

select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 4907 still missing


--4.2
with base as (
select p.property_dwid , dpnl.dw_property_id 
from masterdata_hk.property p
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.dm_property_new_launch dpnl on dpnl.dw_project_id = p.dw_project_id 
	--and dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.address_building) = lower(dpnl.block_num)
where p.dw_property_id isnull
)
select *
from base 
where dw_property_id notnull ;


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.address a on p.address_dwid = a.address_dwid 
left join masterdata_hk.dm_property_new_launch dpnl on dpnl.dw_project_id = p.dw_project_id 
	--and dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.address_building) = lower(dpnl.block_num)
where p.dw_property_id isnull
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
; -- fix 43 missing dw_property_id

select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 4864 still missing

--4.3

with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '7630995aacc43b1b00b9a59a093de11e'
where p.dw_property_id isnull and p.project_dwid = 'c81bd0c4836aac9baab8602370ab2844' -- new launch project: the quinn．square mile
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
; -- fix 614 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '5a3eb44763c715c0927e2109a32433b0'
where p.dw_property_id isnull and p.project_dwid = '1011cadcae3c46d83d1e5aa358986071' -- new launch project: kennedy 38
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 341 missing dw_property_id



with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '6a3797c5d0e65bfe9985e8c946048b28'
where p.dw_property_id isnull and p.project_dwid = '324a47e051a361a4f4bb3bb7b4eec2b8' -- new launch project: the harmonie
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 337 missing dw_property_id



with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '5ff997abdd95094b6ac39be3ae4984de'
where p.dw_property_id isnull and p.project_dwid = '734f6c82296f609d5fd6478828657ac0' -- new launch project: vau residence
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 165 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '4c974bf2f5bf228a40e9ff7503728c83'
where p.dw_property_id isnull and p.project_dwid = 'a808b593cdedfa1f4ae34678bf5e76dc' -- new launch project: the met. azure
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 320 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '15a7c93340cad96a3f744a663f56966d'
where p.dw_property_id isnull and p.project_dwid = 'b5f044455d6e8458a57c265901d9922f' -- new launch project: allegro
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 190 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '124aed5215009237af453b2c7019f770'
where p.dw_property_id isnull and p.project_dwid = 'b3739956afbed0eab9ab1d9bc9761a45' -- new launch project: the holborn
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 420 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '100c62254f79850cd19c2f0e717c5888'
where p.dw_property_id isnull and p.project_dwid = '3bc815ef0fdb4388abb1215e973522ac' -- new launch project: 10 lasalle
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 73 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '0b3675035d3f4932cfa857163e3e4903'
where p.dw_property_id isnull and p.project_dwid = '2c46961128bb0796a5897e142158dc05' -- new launch project: 128 waterloo
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 110 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = 'f234322114625cbc03b421a773e303dc'
where p.dw_property_id isnull and p.project_dwid = '04a85cf2aef04d983f46143ce40328e1' -- new launch project: 128 waterloo
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 101 missing dw_property_id



with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '93805bcf40f7ab6445cc4e4a726811e9'
where p.dw_property_id isnull and p.project_dwid = '521024e4e3f8146238fcdb0f6a3de7b3' -- new launch project: 128 waterloo
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 324 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '937153f83036dc1728024fc46a436d76'
where p.dw_property_id isnull and p.project_dwid = '1a18347fcdb3c0bc0758c34a83098f15' -- new launch project: central 8
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 99 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '9fe74ad4fb43113cfe0783b3a366ed30'
where p.dw_property_id isnull and p.project_dwid = 'aba5cd74219ddfe50138d52790e8d76a' -- new launch project: central 8
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 42 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '89605206f469a5d2e11a612ac15d1fb5'
where p.dw_property_id isnull and p.project_dwid = 'a445adb8015ed35e939cf88aeb2aba20' -- new launch project: the aperture
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 294 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = 'a80db477fc482f9e668b73358ae37d42'
where p.dw_property_id isnull and p.project_dwid = '5d6914cd747c1a7392639569ce392081' -- new launch project: the concerto
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 95 missing dw_property_id



with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '8f830e9874c44620572e8d1100f05f1c'
where p.dw_property_id isnull and p.project_dwid = 'a93b7cd3176f36ea85b29e1386efcbad' -- new launch project: koko reserve
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 82 missing dw_property_id


with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = 'f55706f5324da9b9d6754de123e9982b'
where p.dw_property_id isnull and p.project_dwid = '6f32f55b2cdae7968c0db07fc379e1c9' -- new launch project: bisney crest
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 21 missing dw_property_id

select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 1164 still missing

--4.4
with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	dpnl.address_floor = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.project_name = a.development || ' ' || a.development_phase
where p.dw_property_id isnull and a.development = 'grand victoria'
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 76 missing dw_property_id

--4.5
with base as (
select p.property_dwid , dpnl.dw_property_id , dpnl.slug
from masterdata_hk.property p
left join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
left join masterdata_hk.dm_property_new_launch dpnl on --dpnl.dw_project_id = p.dw_project_id 
	replace(dpnl.address_floor, '.0', '') = p.address_floor_text and dpnl.address_stack = p.address_stack 
	and lower(a.building_block_number) = lower(dpnl.block_num)
	and dpnl.dw_project_id = '68f86d1ef4abadd35a1668acb047d361'
where p.dw_property_id isnull and p.project_dwid = 'a56b2346c940b7d35714530f0f5d7582' -- new launch project: j loft
)
update masterdata_hk.property p
set (dw_property_id, slug) =
(select m.dw_property_id, m.slug from base m where p.property_dwid = m.property_dwid)
where p.dw_property_id isnull
;-- fix 83 missing dw_property_id


select count(*)
from masterdata_hk.property p 
where dw_property_id isnull; -- 1081 still missing --> they are indeed do NOT have corresponding old properties!!!




-------- integrate new launch sale transaction records into sale_transaction table
select * from masterdata_hk.sale_transaction st limit 100;


insert into masterdata_hk.sale_transaction -- insert 5823 new records
(
	property_dwid,address_dwid,building_dwid,project_dwid,units_sold,sale_type,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,
	settlement_date,purchase_amount,country_code,data_uuid,data_source,dw_property_id,original_slug
)
-- should have 5823 records
with nl_sale_txn_base as (
	select 
		activity_date , activity_amount , st.data_source , st.slug as original_slug, dw_activity_id , 
		st.dw_property_id ,p.dw_building_id, p.dw_address_id, p.dw_project_id, 
		p.address_floor, p.address_stack, p.address_unit, 
		p.bathroom_count, p.bedroom_count, p.gross_floor_area_sqm, p.net_floor_area_sqm, 
		a.full_address_text , b.construction_end_year, pj.tenure_code , prop.property_type_code,
		a.address_dwid , pj.project_dwid , b.building_dwid , prop.property_dwid 
	from masterdata_hk.dm_property_activity_new_launch st 
		left join masterdata_hk.dm_property_new_launch p using (dw_property_id)
		left join masterdata_hk.address a on p.dw_address_id = a.dw_address_id and a.address_type_code = 'point-address' --and lower(p.building_name) = lower(a.address_building)
		left join masterdata_hk.building b on p.dw_building_id = b.dw_building_id  
		left join masterdata_hk.project pj on p.dw_project_id = pj.dw_project_id 
		left join masterdata_hk.property prop on st.dw_property_id = prop.dw_property_id 
		-- on prop.address_dwid  = a.address_dwid and prop.address_floor_text = p.address_floor and prop.address_stack = p.address_stack 
	where st.activity_type = 'sale_transaction'
	-- 5823
)
, nl_sale_txn as (
select 
	--id,
	--activity_dwid,
	base.property_dwid , 
	base.address_dwid , 
	base.building_dwid , 
	base.project_dwid , 
	-- null as lot_group_dwid,
	-- null as current_lot_group_dwid,
	-- null as activity_name,
	1::int as units_sold,
	case when hmrst.mkt_type = 2 then 'resale'
		--when hmrst.mkt_type = 1 then 'new-sale'
		else 'new-sale' end as sale_type,
	-- null as sale_subtype,	
	base.construction_end_year as property_completion_year,
	base.property_type_code as property_type_code,
	base.address_unit as address_unit,
	base.full_address_text as address_local_text,
	base.tenure_code as tenure_code,
	base.bathroom_count::int as bathroom_count,
	base.bedroom_count::int as bedroom_count,
	base.gross_floor_area_sqm::float as gross_floor_area_sqm,
	base.net_floor_area_sqm::float as net_floor_area_sqm,
	-- null as land_area_sqm,
	-- null as contract_date,
	base.activity_date as settlement_date, -- update_date
	base.activity_amount as purchase_amount,
	'cn' as country_code,
	hdt.data_uuid::uuid as data_uuid,
	-- null as data_source_uuid,
	base.data_source as data_source,
	base.dw_property_id as dw_property_id ,
	base.original_slug as original_slug
	-- column for test:
	, hdt.mem_no
	, hmrst.data_uuid as midland_data_uuid
	, row_number() over(partition by base.original_slug order by hdt.mem_no, hdt.delivery_date desc, hmrst.data_uuid desc) as seq
from nl_sale_txn_base base 
	left join source.hk_daily_transaction hdt on base.activity_date = hdt.instrument_date 
		and hdt.salelable_floor_area*hdt.net_psf::int = base.activity_amount and base.address_floor = hdt.floor 
		and base.data_source = 'hk-daily-transaction'
	left join source.hk_midland_realty_sale_transaction hmrst on hdt.mem_no::text = split_part(hmrst.id, left(hmrst.id, 4),2)
order by base.original_slug
)-- 7000+ --> 5878
select 
	property_dwid,address_dwid,building_dwid,project_dwid,units_sold,sale_type,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,
	settlement_date,purchase_amount,country_code,data_uuid,data_source,dw_property_id,original_slug
from nl_sale_txn
where seq =1
; -- 5823


-- use it to update address_dwid in property table for those new launch units
update masterdata_hk.property p
set address_dwid =
(select m.address_dwid from masterdata_hk.sale_transaction m where p.property_dwid = m.property_dwid and m.activity_dwid isnull and m.property_dwid notnull)
where p.property_dwid in (select property_dwid from masterdata_hk.sale_transaction where property_dwid notnull and activity_dwid isnull)
;-- fix 4774 missing address_dwid


-- fix wrong address_dwid in sale_transaction
select st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.address a on st.address_dwid = a.address_dwid 
left join masterdata_hk.building b on st.building_dwid = b.building_dwid 
where st.activity_dwid isnull
and lower(a.address_building) != lower(b.building_block_number) ; -- 2497

with fix_base as (
select b.address_dwid as correct_address_dwid, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.address a on st.address_dwid = a.address_dwid 
left join masterdata_hk.building b on st.building_dwid = b.building_dwid 
--left join masterdata_hk.property p on st.property_dwid = p.property_dwid 
where st.activity_dwid isnull
and lower(a.address_building) != lower(b.building_block_number)
)
, fix as (
select correct_address_dwid, property_dwid
from fix_base
)
update masterdata_hk.sale_transaction st
set address_dwid = 
(select correct_address_dwid from fix f where st.property_dwid = f.property_dwid)
where st.activity_dwid isnull


-- use sale_transaction address_dwid to update property table
update masterdata_hk.property p
set address_dwid =
(select m.address_dwid from masterdata_hk.sale_transaction m where p.property_dwid = m.property_dwid and m.activity_dwid isnull and m.property_dwid notnull)
where p.property_dwid in (select property_dwid from masterdata_hk.sale_transaction where property_dwid notnull and activity_dwid isnull)
;



UPDATE masterdata_hk.sale_transaction 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; 











