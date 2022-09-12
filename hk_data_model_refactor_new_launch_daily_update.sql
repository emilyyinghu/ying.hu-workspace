-------- integrate latest new launch records into core tables

-- 1.check whether have new records
select a.*
from reference.hk_new_launch_schematic a 
left join premap_hk.new_launch_schematic_to_dwid b on a.id = b.schematic_id 
where b.schematic_id isnull;

-- 2.create temp table to store the latest records for keeping trace of history
create table staging_hk.new_launch_schematic_to_dwid_20220901 as -- new_launch_schematic_to_dwid_{{ next_ds_nodash }}
select 
	meta.project_name as original_project_name,
	case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
		when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
		then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
		else replace(replace(meta.project_name, 'baker circle．dover', 'baker circle'), 'the quinn．square mile', 'the quinn square mile')
		end as project_name, -- the project name cleaning may need do some updates or manual fix
	case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
		when meta.project_name ilike '% iii%' then 'iii'
		when meta.project_name ilike '% ii%' then 'ii'
		when meta.project_name ilike '% i%' then 'i'
		end as phase,
	sch.block as building_name,
	initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
	meta.address_street ,
	meta.address_num,
	sch.floor, 
	sch.stack,
	sch.id as schematic_id
from reference.hk_new_launch_schematic sch
	left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	where not exists (select 1 from premap_hk.new_launch_schematic_to_dwid premap where premap.schematic_id = sch.id) -- and premap.project_dwid notnull)
group by 1,2,3,4,5,6,7,8,9,10 order by 1,2,3,4,5,6,7,8,9,10
; -- 4695

'''
insert into staging_hk.new_launch_schematic_to_dwid_20220901
select 
	meta.project_name as original_project_name,
	case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
		when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
		then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
		else replace(replace(meta.project_name, 'baker circle．dover', 'baker circle'), 'the quinn．square mile', 'the quinn square mile')
		end as project_name, -- the project name cleaning may need do some updates or manual fix
	case when meta.project_name ilike '%phase%' then 'phase ' || trim(split_part(lower(meta.project_name), 'phase', 2))
		when meta.project_name ilike '% iii%' then 'iii'
		when meta.project_name ilike '% ii%' then 'ii'
		when meta.project_name ilike '% i%' then 'i'
		end as phase,
	sch.block as building_name,
	initcap(coalesce(sch.block, meta.address_num::varchar)) as block_num,
	meta.address_street ,
	meta.address_num,
	sch.floor, 
	sch.stack,
	sch.id as schematic_id
from reference.hk_new_launch_schematic sch
	left join reference.hk_new_launch_meta meta on lower(sch.project) = lower(meta.project_name)
	where meta.project_name = 'university heights'
group by 1,2,3,4,5,6,7,8,9,10 order by 1,2,3,4,5,6,7,8,9,10
;
'''


-- 3.insert latest schematic records into premap table
insert into premap_hk.new_launch_schematic_to_dwid
(
	original_project_name,project_name,phase,building_name,block_num,address_street,address_num,floor,stack,schematic_id
)
select 
	original_project_name,project_name,phase,building_name,block_num,address_street,address_num,floor,stack,schematic_id
from staging_hk.new_launch_schematic_to_dwid_20220901
; -- 4695


-- 4.for projects / units existing in our core table, need to map their dwids
with base as (
select p.project_dwid , a.schematic_id 
from staging_hk.new_launch_schematic_to_dwid_20220901 a
left join masterdata_hk.project p on a.project_name =p.project_name_text 
)
update premap_hk.new_launch_schematic_to_dwid a
set project_dwid = b.project_dwid
from base b where a.id = b.schematic_id and a.project_dwid isnull and b.project_dwid notnull
; -- 0

with base as (
select ad.address_dwid  , a.schematic_id 
from staging_hk.new_launch_schematic_to_dwid_20220901 a
left join masterdata_hk.address ad on lower(a.project_name) = lower(ad.development)
and lower(a.address_street) = lower(ad.street_name) and lower(a.address_num) = lower(ad.address_number)
)
update premap_hk.new_launch_schematic_to_dwid a
set address_dwid  = b.address_dwid
from base b where a.id = b.schematic_id and a.address_dwid isnull and b.address_dwid notnull
; -- 0


-- use project name, address street to check in project and address table whether they indeed don't have existing records
select project_name, phase , building_name , address_street , address_num  
from premap_hk.new_launch_schematic_to_dwid
where project_dwid isnull
group by 1,2,3,4,5 order by 1,2;

-- '10 wang fung terrace' -> it's a new project rather than 'mayflower mansion'


-- !!! actually most of them are from manual input and not exist in current core table so need to insert corresponding entities in each core tables


-- 5.create new project records for new launch projects; create new address records for new launch address; 

select metadata.fn_create_change_request(
    'hk-create-new-launch-project-records-in-project-2022-09-01', 'huying','huying' -- 'pipeline', 'pipeline'
); --641

call metadata.sp_add_change_table(641::int, 'hk', replace('project', '-', '_'));


select metadata.fn_create_change_request(
    'hk-create-new-launch-address-records-in-address-2022-09-01', 'huying','huying' -- 'pipeline', 'pipeline'
); --642

call metadata.sp_add_change_table(642::int, 'hk', replace('address', '-', '_'));



select project_name 
from premap_hk.new_launch_schematic_to_dwid
where project_dwid isnull
group by 1


select project_name, phase , building_name , address_street , address_num  
from premap_hk.new_launch_schematic_to_dwid
where project_dwid isnull
group by 1,2,3,4,5 order by 1,2


-- pay attention to the slug format change; dw_address_id could leave them NULL or follow old logic
insert into branch_hk.address_cr_642 
(
	address_type_code,address_type_attribute,full_address_text,address_display_text,address_building,address_number,
	address_street_text,street_name,street_name_root,
	development,development_phase,metro_area,metro_area_code,country,country_code,country_3code,geographic_zone,continent,--slug,
	status_code,language_code,data_source_count,hash_key,dw_address_id,data_source,cr_record_action
)
(
with point_address_base as (
select project_name, lower(phase) as phase , lower(building_name) as building_name , lower(address_street) as address_street , address_num  
from premap_hk.new_launch_schematic_to_dwid
where address_dwid isnull --project_dwid isnull
group by 1,2,3,4,5 order by 1,2
)
select 
	--id,
	--address_dwid,
	'point-address' as address_type_code,
	null as address_type_attribute,
	lower(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(building_name,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as full_address_text,
	initcap(trim(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')||','||coalesce(building_name,'')||','||
		coalesce(phase,'')||','||coalesce(project_name,'')||','||'hong kong (sar)', ','))) as address_display_text,
	lower(building_name) as address_building,
	address_num as address_number,
	lower(trim(coalesce(address_num,'')||' '||coalesce(address_street,''))) as address_street_text,
	lower(address_street) as street_name,
	lower(address_street) as street_name_root,
	lower(project_name) as development,
	lower(phase) as development_phase,
	'hong kong (sar)' as metro_area,
	'hk' as metro_area_code,
	'china' as country,
	'cn' as country_code,
	'chn' as country_3code,
	'south-east asia' as geographic_zone,
	'asia' as continent,
	--latitude,longitude,location_marker,
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'/'||coalesce(phase,'')||'/'||
 	    coalesce(building_name,'')||'/'||coalesce(trim(coalesce(address_num,'')||' '||coalesce(address_street,'')),'') 
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) as dw_address_id,
 	'rea-manual' as data_source,
 	'insert' as cr_record_action
from point_address_base
) -- 68
union
(
with project_address_base as (
select project_name, lower(address_street) as address_street
from premap_hk.new_launch_schematic_to_dwid
where address_dwid isnull --project_dwid isnull
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
	lower(address_street) as address_street_text,
	lower(address_street) as street_name,
	lower(address_street) as street_name_root,
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
 	'active' as status_code,
 	'eng' as language_code,
 	'1'::int as data_source_count,
 	md5(trim(trim(lower(replace(replace(replace(replace(regexp_replace('cn/hk/'||'/'||coalesce(project_name,'')||'///'||coalesce(address_street,'')
 	    , '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '/'))) as hash_key,
 	md5(lower('hk__'|| address_street || '__' || project_name)) AS dw_address_id,
 	'rea-manual' as data_source,
 	'insert' as cr_record_action
from project_address_base
) -- 9
; -- 77


UPDATE branch_hk.address_cr_642
SET slug = trim(trim(lower(replace(replace(replace(replace(regexp_replace('poi/hk/address/'||coalesce(address_building,'')||'-'||
	coalesce(trim(coalesce(address_number,'')||' '||coalesce(street_name,'')),'')||'-'||id, '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '-'))
WHERE address_type_code = 'point-address' and slug isnull ;


UPDATE branch_hk.address_cr_642
SET slug = trim(trim(lower(replace(replace(replace(replace(regexp_replace('poi/hk/project/'||coalesce(development,'')||'-'||id, '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-')), '-'))
WHERE address_type_code = 'project-address' and slug isnull ;


UPDATE branch_hk.address_cr_642
SET address_dwid = md5(country_code||'__'||'address'||'__'||id) 
WHERE address_dwid isnull ;




insert into branch_hk.project_cr_641 
(
	address_dwid,project_type_code,project_display_name,project_name_text,completion_year,tenure_code,
	unit_count,residential_unit_count,commercial_unit_count,address_display_text,location_marker,
	country_code,is_active,data_source,cr_record_action
)
with project_unit_cnt_base as (
select a.project_name , meta.total_units
from premap_hk.new_launch_schematic_to_dwid a
left join reference.hk_new_launch_meta meta 
	on lower(a.original_project_name) = lower(meta.project_name)
where a.project_dwid isnull
group by 1,2
)
, project_unit_cnt as (
select project_name, sum(total_units) as total_units
from project_unit_cnt_base
group by 1
)
, dedup as (
select 
	--id,
	--project_dwid,
	ad.address_dwid ,
	case when meta.property_type ilike 'hos' then 'hos'
		else 'condo' end as project_type_code,
	--project_name,
	initcap(a.project_name) as project_display_name, 
	lower(a.project_name) as project_name_text,
	EXTRACT('year' FROM meta.completion_year::date)::int as completion_year,
	'leasehold' as tenure_code,
	cnt.total_units as unit_count,
	cnt.total_units as residential_unit_count,
	null::int as commercial_unit_count,
	ad.address_display_text,
	st_astext(st_makepoint(meta.location_lon, meta.location_lat)) as location_marker,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	'rea-manual' as data_source,
	'insert' as cr_record_action,
	row_number() over (partition by a.project_name order by meta.completion_year) as seq
from premap_hk.new_launch_schematic_to_dwid a
left join reference.hk_new_launch_meta meta 
	on lower(a.original_project_name) = lower(meta.project_name)
left join branch_hk.address_cr_642 ad -- masterdata_hk.address
	on lower(a.project_name) = lower(ad.development) and lower(a.address_street) = lower(ad.street_name) and ad.address_type_code = 'project-address' and ad.city isnull 
left join project_unit_cnt cnt on lower(a.project_name) =lower(cnt.project_name)
where a.project_dwid isnull
group by 1,2,3,4,5,7,10,11,a.project_name,meta.completion_year
)
select
	address_dwid,project_type_code,project_display_name,project_name_text,completion_year,
	tenure_code,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	location_marker,country_code,is_active,data_source,cr_record_action
from dedup
where seq = 1
; -- 9


UPDATE branch_hk.project_cr_641 
SET project_name = api.get_normalized_name(project_name_text) 
WHERE project_name isnull ;


UPDATE branch_hk.project_cr_641 
SET project_dwid = md5(country_code||'__'||'project'||'__'||id) WHERE project_dwid isnull ;

UPDATE branch_hk.project_cr_641 
SET slug = api.clean_slug('project/hk/'||coalesce(project_type_code,'')||'/'||coalesce(project_name,'')||'-'||id) 
WHERE slug isnull ;
 


call metadata.sp_submit_change_request(642, 'huying');

call metadata.sp_approve_change_request(642, 'huying');

call metadata.sp_merge_change_request(642);

call metadata.sp_submit_change_request(641, 'huying');

call metadata.sp_approve_change_request(641, 'huying');

call metadata.sp_merge_change_request(641);

'''
select a.*
from masterdata_hk.address a 
left join branch_hk.address_cr_642 b on a.id = b.id 
where b.id notnull;

update masterdata_hk.address a 
set data_source = 'rea-manual'
where exists (select 1 from branch_hk.address_cr_642 b where a.id = b.id)
; -- 77
'''

-- 6.create new building records for new launch buildings;

select metadata.fn_create_change_request(
    'hk-create-new-launch-building-records-in-building-2022-09-06', 'huying','huying' -- 'pipeline', 'pipeline'
); --702

call metadata.sp_add_change_table(702::int, 'hk', replace('building', '-', '_'));


insert into branch_hk.building_cr_702
(
	address_dwid,project_dwid,lot_group_dwid,building_block_number,building_display_name,building_name_text,
	construction_end_year,unit_count,residential_unit_count,commercial_unit_count,address_display_text,
	country_code,is_active,data_source,cr_record_action
)
with building_unit_cnt as (
select project_name, lower(phase) as phase, lower(building_name) as building_name, lower(address_street) as address_street, address_num, count(distinct schematic_id) as unit_count
from premap_hk.new_launch_schematic_to_dwid
where building_dwid isnull --project_dwid isnull 
and building_name notnull
group by 1,2,3,4,5 order by 1,2 -- 65 + 6
)
select 
	--id,
	--building_dwid,
	ad.address_dwid as address_dwid,
	p.project_dwid as project_dwid,
	null as lot_group_dwid,
	-- building_name,
	initcap(a.block_num) as building_block_number,
	initcap(a.block_num) as building_display_name,
	lower(a.block_num) as building_name_text,
	p.completion_year as construction_end_year,
	cnt.unit_count as unit_count,
	cnt.unit_count as residential_unit_count,
	null::int as commercial_unit_count,
	lower(ad.address_display_text) as address_display_text,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	'rea-manual' as data_source,
	'insert' as cr_record_action
from premap_hk.new_launch_schematic_to_dwid a
left join masterdata_hk.address ad --branch_hk.address_cr_642 ad -- masterdata_hk.address
	on lower(a.project_name) = lower(ad.development) and lower(a.address_street) = lower(ad.street_name)
	and lower(a.block_num) = lower(coalesce(ad.address_building, ad.address_number)) and lower(a.address_num) = lower(ad.address_number)
	and ad.address_type_code = 'point-address'
left join masterdata_hk.project p on lower(a.project_name) = lower(p.project_name_text)
left join building_unit_cnt cnt on lower(a.project_name) = lower(cnt.project_name) and f_prep_dw_id(a.phase) = f_prep_dw_id(cnt.phase) and lower(a.building_name) = lower(cnt.building_name)
where a.building_dwid isnull --a.project_dwid isnull 
and a.building_name notnull
group by 1,2,a.block_num,7,8,11
; -- 65 + 6


UPDATE branch_hk.building_cr_702
SET building_name = api.get_normalized_name(building_name_text) 
WHERE building_name isnull 
;

UPDATE branch_hk.building_cr_702
SET building_dwid = md5(country_code||'__'||'building'||'__'||id) 
WHERE building_dwid isnull 
;


update branch_hk.building_cr_702 a
SET slug = api.clean_slug(b.slug||'/'||coalesce(a.building_name,'')||'-'||a.id)
from masterdata_hk.project b where a.project_dwid = b.project_dwid
and a.slug isnull ;
 


call metadata.sp_submit_change_request(702, 'huying');

call metadata.sp_approve_change_request(702, 'huying');

call metadata.sp_merge_change_request(702);


-- check:
select *
from masterdata_hk.building 
where building_name_text isnull;

-- 7.create new property records for new launch units;

select metadata.fn_create_change_request(
    'hk-create-new-launch-property-records-in-property-2022-09-06', 'huying','huying' -- 'pipeline', 'pipeline'
); --703

call metadata.sp_add_change_table(703::int, 'hk', replace('property', '-', '_'));


insert into branch_hk.property_cr_703
(
	address_dwid,building_dwid,project_dwid,property_type_code,address_unit,address_floor_text,address_floor_num,address_stack,
	bedroom_count,bathroom_count,net_floor_area_sqm,gross_floor_area_sqm,country_code,is_active,data_source,cr_record_action
)
with unit_base as (
select project_name, lower(phase) as phase, lower(building_name) as building_name, block_num, lower(address_street) as address_street, address_num, floor , stack , schematic_id 
from premap_hk.new_launch_schematic_to_dwid
where property_dwid isnull --project_dwid isnull
)
select 
	--id,
	--property_dwid,
	ad.address_dwid, --a.block_num,
	b.building_dwid,
	p.project_dwid,
	p.project_type_code as property_type_code,
	a.floor||'-'||a.stack as address_unit,
	a.floor as address_floor_text,
	case when a.floor = 'G' then 0::int else a.floor::int end as address_floor_num,
	a.stack as address_stack,
	sch.num_of_bedrooms as bedroom_count,
	sch.num_of_bathrooms as bathroom_count,
	sch.built_up_area_sqm as net_floor_area_sqm,
	sch.floor_area_sqm as gross_floor_area_sqm,
	--slug,
	'cn' as country_code,
	true::boolean as is_active,
	'rea-manual' as data_source,
	'insert' as cr_record_action
from unit_base a
left join masterdata_hk.address ad
	on lower(a.project_name) = lower(ad.development) and lower(a.address_street) = lower(ad.street_name)
	and lower(a.block_num) = lower(coalesce(ad.address_building, ad.address_number)) and lower(a.address_num) = lower(ad.address_number)
	and ad.address_type_code = 'point-address'
left join masterdata_hk.project p on lower(a.project_name) = lower(p.project_name_text)	and lower(a.address_street) = lower(split_part(p.address_display_text, ',', 1)) 
left join masterdata_hk.building b on ad.address_dwid = b.address_dwid and lower(a.block_num) = lower(b.building_block_number) and p.project_dwid = b.project_dwid 
left join reference.hk_new_launch_schematic sch on a.schematic_id = sch.id 
; -- 4695 + 75


UPDATE branch_hk.property_cr_703 
SET property_dwid = md5(country_code||'__'||'property'||'__'||id) 
WHERE property_dwid isnull ;

with base as (
select 
	a.id, 
	api.clean_slug(b.slug||'/'||coalesce(c.building_name,'')||'/'||coalesce(a.address_floor_text,'')||'/'||coalesce(a.address_stack,'')||'-'||a.id) as slug
from branch_hk.property_cr_703 a
left join masterdata_hk.project b on a.project_dwid = b.project_dwid
left join masterdata_hk.building c on a.building_dwid = c.building_dwid
where a.slug isnull 
)
UPDATE branch_hk.property_cr_703 a
set slug = b.slug
from base b where a.id = b.id; -- 4695 + 75



call metadata.sp_submit_change_request(703, 'huying');

call metadata.sp_approve_change_request(703, 'huying');

call metadata.sp_merge_change_request(703);


-- 8.populate dwids in premap_hk.new_launch_schematic_to_dwid

with dwid_base as (
select a.id, cr.property_dwid, cr.address_dwid, cr.building_dwid , cr.project_dwid 
from premap_hk.new_launch_schematic_to_dwid a
left join masterdata_hk.address ad -- masterdata_hk.address
	on lower(a.project_name) = lower(ad.development) and lower(a.address_street) = lower(ad.street_name)
	and lower(a.block_num) = lower(coalesce(ad.address_building, ad.address_number)) and lower(a.address_num) = lower(ad.address_number)
	and ad.address_type_code = 'point-address'
left join masterdata_hk.project p on lower(a.project_name) = lower(p.project_name_text) and lower(a.address_street) = lower(split_part(p.address_display_text, ',', 1)) 
left join masterdata_hk.building b on ad.address_dwid = b.address_dwid and lower(a.block_num) = lower(b.building_block_number) and p.project_dwid = b.project_dwid 
left join reference.hk_new_launch_schematic sch on a.schematic_id = sch.id 
left join branch_hk.property_cr_703 cr 
on f_prep_dw_id(ad.address_dwid) = f_prep_dw_id(cr.address_dwid) 
and f_prep_dw_id(b.building_dwid) = f_prep_dw_id(cr.building_dwid) 
and f_prep_dw_id(p.project_dwid) = f_prep_dw_id(cr.project_dwid) 
and f_prep_dw_id(a.floor) = f_prep_dw_id(cr.address_floor_text) 
and f_prep_dw_id(a.stack) = f_prep_dw_id(cr.address_stack) 
where a.property_dwid isnull -- a.project_dwid isnull -- 4695 + 75
)
update premap_hk.new_launch_schematic_to_dwid a
set project_dwid = b.project_dwid, 
address_dwid = b.address_dwid,
building_dwid = b.building_dwid,
property_dwid = b.property_dwid
from dwid_base b
where a.id = b.id; --4695 + 75


-- check if still have missing project / address / building / property, if yes then we need to manually add them (or fix the logic)
select *
from premap_hk.new_launch_schematic_to_dwid
where project_dwid isnull; -- 0

select *
from premap_hk.new_launch_schematic_to_dwid
where project_dwid notnull and property_dwid isnull; -- university heights

select project_dwid notnull, building_dwid notnull, address_dwid notnull, property_dwid notnull, count(*)
from premap_hk.new_launch_schematic_to_dwid
group by 1,2,3,4;
'''
true	false	true	true	308
true	true	true	true	22827
'''

-- 9.insert new records into unit_group and update unit_group_dwid in property table
select metadata.fn_create_change_request(
	'hk-create-new-launch-unit_group-records-in-property-2022-09-06', 'huying','huying' -- 'pipeline', 'pipeline'
); --704

call metadata.sp_add_change_table(704::int, 'hk', replace('unit_group', '-', '_'));



insert into branch_hk.unit_group_cr_704
(
	unit_group_dwid,project_dwid,unit_group,unit_type,unit_subtype_code,
	gross_floor_area_sqm,gross_floor_area_sqft,net_floor_area_sqm,net_floor_area_sqft,
	unit_count,bedroom_count,bathroom_count,other_room_count,unit_group_order,is_penthouse,
	country_code,property_type_code, cr_record_action
)
select
	null as unit_group_dwid,
	b.project_dwid ,
	lower(b.project_name || coalesce(' - ' || b.phase, '') || ' - ' || a.unit_model) as unit_group,
	a.unit_layout as unit_type,
	lower(a.unit_model) as unit_subtype_code,
	built_up_area_sqm as gross_floor_area_sqm,
 	built_up_area_sqft as gross_floor_area_sqft,
 	floor_area_sqm as net_floor_area_sqm,
 	floor_area_sqft as net_floor_area_sqft,
 	count(*) as unit_count,
 	num_of_bedrooms as bedroom_count,
 	num_of_bathrooms as bathroom_count,
 	case when unit_layout like '%with%and%' and unit_layout not ilike '%suite%' then 2::int 
 	 	when unit_layout like '%with%and%' and unit_layout ilike 'with%suite%and%suite%' then null
 	 	when unit_layout like '%with%and%' and (unit_layout ilike 'with%and%suite%' or unit_layout ilike 'with%suite%and%') then 1::int
 		when unit_layout like '%with%' then 1::int 
 		else null end as other_room_count,
 	null::int as unit_group_order,
 	is_penthouse,
 	'hk' as country_code,
 	case when c.property_type ilike 'hos' then 'hos'
 		else 'condo' end as property_type_code,
 	'insert' as cr_record_action
from reference.hk_new_launch_schematic a
left join premap_hk.new_launch_schematic_to_dwid b on a.id = b.schematic_id 
left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
left join reference.hk_new_launch_meta c on a.project = c.project_name 
where b.schematic_id notnull and cr.schematic_id notnull
group by 2,3,4,5,6,7,8,9,11,12,13,15,17
;  -- 640 + 37


update branch_hk.unit_group_cr_704
set unit_group_dwid = api.get_dwid(country_code, 'unit_group', id)
where unit_group_dwid isnull; -- 640


with base as (
select 
	ug.id,
	trim(trim(replace(replace('project/hk/'||ug.property_type_code||'/'||replace(trim(replace(split_part(ug.unit_group, reverse(split_part(reverse(ug.unit_group), ' - ', 1)), 1), ' - ', ' ')), ' ', '-')
		||'-'||p.id||'/'||ug.bedroom_count||'/'||replace(replace(ug.unit_subtype_code, ' ', '-'), '/', '-')||'-'||coalesce(ug.gross_floor_area_sqm::text, ''),'(',''),')',''), '-')) as slug
from branch_hk.unit_group_cr_704 ug
left join masterdata_hk.project p on ug.project_dwid = p.project_dwid 
)
update branch_hk.unit_group_cr_704 a
set slug = b.slug
from base b where a.id = b.id 
and a.slug isnull
;

call metadata.sp_submit_change_request(704, 'huying');

call metadata.sp_approve_change_request(704, 'huying');

call metadata.sp_merge_change_request(704);

'''
insert into masterdata_hk.unit_group 
(
	id,unit_group_dwid,project_dwid,unit_group,unit_type,unit_subtype_code,gross_floor_area_sqm,gross_floor_area_sqft,
	net_floor_area_sqm,net_floor_area_sqft,unit_count,bedroom_count,bathroom_count,other_room_count,unit_group_order,is_penthouse,
	country_code,property_type_code,slug
)
select 
	id,unit_group_dwid,project_dwid,unit_group,unit_type,unit_subtype_code,gross_floor_area_sqm,gross_floor_area_sqft,
	net_floor_area_sqm,net_floor_area_sqft,unit_count,bedroom_count,bathroom_count,other_room_count,unit_group_order,is_penthouse,
	country_code,property_type_code,slug
from branch_hk.unit_group_cr_704
where project_dwid = '36c8f3a0d16e029c3f4bde11035f7e44'; --37

update branch_hk.unit_group_cr_704
set country_code = 'hk'
where country_code = 'cn'; -- 677
'''


select metadata.fn_create_change_request(
    'hk-update-new-launch-unit_group_dwid-in-property-2022-09-12', 'huying','huying' -- 'pipeline', 'pipeline'
); --762

call metadata.sp_add_change_table(762::int, 'hk', replace('property', '-', '_'));

insert into branch_hk.property_cr_705
with unit_group_dwidbase as (
	select b.property_dwid, ug.unit_group_dwid
	from premap_hk.new_launch_schematic_to_dwid b
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
	left join masterdata_hk.unit_group ug
	on lower(b.project_name || coalesce(' - ' || b.phase, '') || ' - ' || a.unit_model) = ug.unit_group 
	and a.unit_layout = ug.unit_type 
	and a.num_of_bedrooms = ug.bedroom_count 
	and f_prep_dw_id(a.built_up_area_sqm::text) = f_prep_dw_id(ug.gross_floor_area_sqm::text)
	and f_prep_dw_id(a.built_up_area_sqft::text) = f_prep_dw_id(ug.gross_floor_area_sqft::text)
	where b.property_dwid notnull and cr.schematic_id notnull -- 4695
)
select 
	p.id,p.property_dwid,p.address_dwid,p.building_dwid,b.unit_group_dwid,p.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join unit_group_dwidbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull and p.unit_group_dwid isnull; -- 4695


-- should be consistent:
select sum(unit_count) as cnt from branch_hk.unit_group_cr_704; -- 4695


call metadata.sp_submit_change_request(705, 'huying');

call metadata.sp_approve_change_request(705, 'huying');

call metadata.sp_merge_change_request(705);


'''
insert into branch_hk.property_cr_762
with unit_group_dwidbase as (
	select b.property_dwid, ug.unit_group_dwid, f_prep_dw_id(a.built_up_area_sqm::text), f_prep_dw_id(ug.gross_floor_area_sqm::text)
	from premap_hk.new_launch_schematic_to_dwid b
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join masterdata_hk.unit_group ug
	on lower(b.project_name || coalesce(' - ' || b.phase, '') || ' - ' || a.unit_model) = ug.unit_group 
	and a.unit_layout = ug.unit_type 
	and a.num_of_bedrooms = ug.bedroom_count 
	and f_prep_dw_id(a.built_up_area_sqm::text) = f_prep_dw_id(ug.gross_floor_area_sqm::text)
	and f_prep_dw_id(a.built_up_area_sqft::text) = f_prep_dw_id(ug.gross_floor_area_sqft::text)
	where b.project_dwid = '36c8f3a0d16e029c3f4bde11035f7e44' -- 75
)
select 
	p.id,p.property_dwid,p.address_dwid,p.building_dwid,b.unit_group_dwid,p.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join unit_group_dwidbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull and p.unit_group_dwid isnull; -- 75

select sum(unit_count) as cnt from branch_hk.unit_group_cr_704 where project_dwid = '36c8f3a0d16e029c3f4bde11035f7e44'; -- 75

call metadata.sp_submit_change_request(762, 'huying');

call metadata.sp_approve_change_request(762, 'huying');

call metadata.sp_merge_change_request(762);
'''






-- 10.TO DO: last step: create new sale transaction / sale listing / rent transaction records for new launch units;

select metadata.fn_create_change_request(
    'hk-create-new-launch-sale-transaction-records-in-sale-transaction-2022-09-02', 'huying','huying' -- 'pipeline', 'pipeline'
); --666

call metadata.sp_add_change_table(666::int, 'hk', replace('sale-transaction', '-', '_'));

'''
select p.property_dwid, p.address_floor_text notnull, a.activity_type, a.activity_date, a.developer_price
from masterdata_hk.property p 
left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
where b.property_dwid notnull and cr.schematic_id notnull;
'''

insert into branch_hk.sale_transaction_cr_666
(
	property_dwid,address_dwid,building_dwid,project_dwid,units_sold,sale_type,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,
	settlement_date,purchase_amount,country_code,data_uuid,data_source,cr_record_action
)
with activity_base as (
	select
		a.id,
		case when p.address_floor_text notnull then (
			case when (a.developer_price isnull and a.activity_type = 'sale') or a.sale_status = 'Sold' then 'sale_transaction'
				when a.developer_price notnull then 'sale_listing'
				when a.developer_price isnull and a.activity_type = 'rent' then 'rent_transaction'
			end)
		when p.address_floor_text isnull then (
			case when a.activity_type = 'sale' then 'sale_transaction'
				when a.activity_type = 'rent' then 'rent_transaction'
				when a.developer_price notnull then 'sale_listing'
			end)
		end as activity_type
	from masterdata_hk.property p 
	left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
	where b.property_dwid notnull and cr.schematic_id notnull
)
select 
	p.property_dwid,
	p.address_dwid ,
	p.building_dwid ,
	p.project_dwid ,
	1::int as units_sold,
	'new-sale' as sale_type,
	pj.completion_year as property_completion_year,
	p.property_type_code,
	p.address_unit ,
	ad.full_address_text as address_local_text,
	pj.tenure_code ,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	a.activity_date as settlement_date,
	a.developer_price as purchase_amount,
	'cn' as country_code,
	null as data_uuid,
	'rea-manual' as data_source,
	'insert' as cr_record_action
from masterdata_hk.property p 
left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid
left join masterdata_hk.address ad on p.address_dwid = ad.address_dwid 
left join activity_base base on a.id = base.id and base.activity_type = 'sale_transaction'
where b.property_dwid notnull and cr.schematic_id notnull and base.id notnull
; --2855


UPDATE branch_hk.sale_transaction_cr_666
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; 

call metadata.sp_submit_change_request(666, 'huying');

call metadata.sp_approve_change_request(666, 'huying');

call metadata.sp_merge_change_request(666);




select metadata.fn_create_change_request(
    'hk-create-new-launch-sale-listing-records-in-sale-listing-2022-09-12', 'huying','huying' -- 'pipeline', 'pipeline'
); --763

call metadata.sp_add_change_table(763::int, 'hk', replace('sale-listing', '-', '_'));


insert into branch_hk.sale_listing_cr_763
(
	property_dwid,address_dwid,building_dwid,project_dwid,
	activity_display_text,sale_type,property_completion_year,property_type_code,address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,first_listing_date,listing_amount,country_code,data_uuid,data_source,cr_record_action
)
with activity_base as (
	select
		a.id,
		case when p.address_floor_text notnull then (
			case when (a.developer_price isnull and a.activity_type = 'sale') or a.sale_status = 'Sold' then 'sale_transaction'
				when a.developer_price notnull then 'sale_listing'
				when a.developer_price isnull and a.activity_type = 'rent' then 'rent_transaction'
			end)
		when p.address_floor_text isnull then (
			case when a.activity_type = 'sale' then 'sale_transaction'
				when a.activity_type = 'rent' then 'rent_transaction'
				when a.developer_price notnull then 'sale_listing'
			end)
		end as activity_type
	from masterdata_hk.property p 
	left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
	where b.property_dwid notnull and cr.schematic_id notnull
)
select 
	p.property_dwid,
	p.address_dwid ,
	p.building_dwid ,
	p.project_dwid ,
	lower(coalesce(p.bedroom_count || ' bdr ', '') || p.property_type_code || ' at ' || coalesce(pj.project_name_text, ad.address_building)) as activity_display_text,
	'sale' as sale_type,
	pj.completion_year as property_completion_year,
	p.property_type_code,
	p.address_unit ,
	ad.full_address_text as address_local_text,
	pj.tenure_code ,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	a.activity_date as first_listing_date,
	a.developer_price as listing_amount,
	'cn' as country_code,
	null as data_uuid,
	'rea-manual' as data_source,
	'insert' as cr_record_action
from masterdata_hk.property p 
left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid
left join masterdata_hk.address ad on p.address_dwid = ad.address_dwid 
left join activity_base base on a.id = base.id and base.activity_type = 'sale_listing'
where b.property_dwid notnull and cr.schematic_id notnull and base.id notnull
; -- 589

UPDATE branch_hk.sale_listing_cr_763
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull;

call metadata.sp_submit_change_request(763, 'huying');

call metadata.sp_approve_change_request(763, 'huying');

call metadata.sp_merge_change_request(763);


select metadata.fn_create_change_request(
    'hk-create-new-launch-rent-transaction-records-in-rent-transaction-2022-09-12', 'huying','huying' -- 'pipeline', 'pipeline'
);

--call metadata.sp_add_change_table(763::int, 'hk', replace('rent-transaction', '-', '_'));


--insert into branch_hk.rent_transaction_cr_763
(
	property_dwid,address_dwid,building_dwid,project_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,rent_start_date,rent_amount_monthly,
	country_code,data_uuid,data_source,cr_record_action
)
with activity_base as (
	select
		a.id,
		case when p.address_floor_text notnull then (
			case when (a.developer_price isnull and a.activity_type = 'sale') or a.sale_status = 'Sold' then 'sale_transaction'
				when a.developer_price notnull then 'sale_listing'
				when a.developer_price isnull and a.activity_type = 'rent' then 'rent_transaction'
			end)
		when p.address_floor_text isnull then (
			case when a.activity_type = 'sale' then 'sale_transaction'
				when a.activity_type = 'rent' then 'rent_transaction'
				when a.developer_price notnull then 'sale_listing'
			end)
		end as activity_type
	from masterdata_hk.property p 
	left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
	where b.property_dwid notnull and cr.schematic_id notnull
)
select 
	p.property_dwid,
	p.address_dwid ,
	p.building_dwid ,
	p.project_dwid ,
	lower(p.property_type_code || ' ' || p.bedroom_count || '-Rm at ' || coalesce(pj.project_name_text, ad.address_building)) as activity_name,
	'rental'::text as rent_type,
	p.property_type_code,
	p.bedroom_count || '-Rm' as property_subtype,
	p.address_unit ,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	a.activity_date as rent_start_date,
	a.developer_price as rent_amount_monthly,
	'cn' as country_code,
	null as data_uuid,
	'rea-manual' as data_source,
	'insert' as cr_record_action
from masterdata_hk.property p 
left join premap_hk.new_launch_schematic_to_dwid b on p.property_dwid = b.property_dwid 
left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
left join staging_hk.new_launch_schematic_to_dwid_20220901 cr on b.schematic_id = cr.schematic_id 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid
left join masterdata_hk.address ad on p.address_dwid = ad.address_dwid 
left join activity_base base on a.id = base.id and base.activity_type = 'rent_transaction'
where b.property_dwid notnull and cr.schematic_id notnull and base.id notnull
; -- 0


UPDATE branch_hk.rent_transaction_cr_763
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; 

--call metadata.sp_submit_change_request(763, 'huying');

--call metadata.sp_approve_change_request(763, 'huying');

--call metadata.sp_merge_change_request(763);


