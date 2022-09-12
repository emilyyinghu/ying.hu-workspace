--  for HK new launch:

-- 1.insert new records into new_launch_schematic_to_dwid - better to create cr to keep trace of the history
insert into premap_hk.new_launch_schematic_to_dwid
(original_project_name,project_name,phase,building_name,block_num,address_street,address_num,floor,stack,schematic_id)
select 
	meta.project_name as original_project_name,
	case when meta.project_name ilike '%phase%' then trim(split_part(lower(meta.project_name), 'phase', 1))
		when meta.project_name ilike '% iii%' or meta.project_name ilike '% ii%' or meta.project_name ilike '% i%' 
		then trim(split_part(split_part(split_part(lower(meta.project_name), ' iii', 1), ' ii', 1), ' i', 1))
		else replace(replace(meta.project_name, 'baker circle．dover', 'baker circle'), 'the quinn．square mile', 'the quinn square mile')
		end as project_name,
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
	where not exists (select 1 from premap_hk.new_launch_schematic_to_dwid premap where premap.schematic_id = sch.id)
group by 1,2,3,4,5,6,7,8,9,10 order by 1,2,3,4,5,6,7,8,9,10
;

-- map dwids

-- check consistency
select project_dwid notnull, building_dwid notnull, address_dwid notnull ,property_dwid notnull, count(*)
from premap_hk.new_launch_schematic_to_dwid
group by 1,2,3,4; -- true false false false 75 --> university heights

-- merge cr into premap_hk.new_launch_schematic_to_dwid

-- 2.create cr to insert new records into unit_group

insert into masterdata_hk.unit_group 
(
	unit_group_dwid,project_dwid,unit_group,unit_type,unit_subtype_code,
	gross_floor_area_sqm,gross_floor_area_sqft,net_floor_area_sqm,net_floor_area_sqft,unit_count,
	bedroom_count,bathroom_count,other_room_count,unit_group_order,is_penthouse,country_code,property_type_code
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
 		else 'condo' end as property_type_code
from reference.hk_new_launch_schematic a
left join `new_launch_schematic_to_dwid cr table` b on a.id = b.schematic_id 
left join reference.hk_new_launch_meta c on a.project = c.project_name 
where b.b.schematic_id notnull
group by 2,3,4,5,6,7,8,9,11,12,13,15,17
; 

-- update unit_group_dwid and slug
update masterdata_hk.unit_group 
set unit_group_dwid = api.get_dwid(country_code, 'unit_group', id)
where unit_group_dwid isnull;

with base as (
select 
ug.id,
trim(trim(replace(replace('project/hk/'||ug.property_type_code||'/'||replace(trim(replace(split_part(ug.unit_group, reverse(split_part(reverse(ug.unit_group), ' - ', 1)), 1), ' - ', ' ')), ' ', '-')
		||'-'||p.id||'/'||ug.bedroom_count||'/'||replace(replace(ug.unit_subtype_code, ' ', '-'), '/', '-')||'-'||coalesce(ug.gross_floor_area_sqm::text, ''),'(',''),')',''), '-')) as slug
from masterdata_hk.unit_group ug
left join masterdata_hk.project p on ug.project_dwid = p.project_dwid 
)
update masterdata_hk.unit_group a
set slug = b.slug
from base b where a.id = b.id 
and a.slug isnull
;

-- 3.create cr to update unit_group_dwid in property table

with unit_group_dwidbase as (
	select b.property_dwid, ug.unit_group_dwid
	from premap_hk.new_launch_schematic_to_dwid b
	left join reference.hk_new_launch_schematic a on a.id = b.schematic_id 
	left join masterdata_hk.unit_group ug
	on lower(b.project_name || coalesce(' - ' || b.phase, '') || ' - ' || a.unit_model) = ug.unit_group 
	and a.unit_layout = ug.unit_type 
	and a.num_of_bedrooms = ug.bedroom_count 
	and f_prep_dw_id(a.built_up_area_sqm::text) = f_prep_dw_id(ug.gross_floor_area_sqm::text)
	and f_prep_dw_id(a.built_up_area_sqft::text) = f_prep_dw_id(ug.gross_floor_area_sqft::text)
	where b.property_dwid notnull
)
select 
	p.id,p.property_dwid,p.address_dwid,p.building_dwid,b.unit_group_dwid,p.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join unit_group_dwidbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull and p.unit_group_dwid isnull;





-- for HK NOT new launch:

insert into branch_hk.unit_group_cr_627
(
	project_dwid,unit_group,unit_type,unit_subtype_code,gross_floor_area_sqm,gross_floor_area_sqft,net_floor_area_sqm,net_floor_area_sqft,
	unit_count,bedroom_count,bathroom_count,other_room_count,country_code,property_type_code, cr_record_action
)
with unit_group_test as (
select p.project_dwid , bedroom_count::int , bathroom_count::int , other_room_count::int , gross_floor_area_sqm::int , net_floor_area_sqm::int , property_type_code , count(*) as unit_count
from masterdata_hk.property p 
where p.unit_group_dwid isnull and p.project_dwid notnull and bedroom_count notnull and bedroom_count != 0
group by 1,2,3,4,5,6,7
)
select 
	--null as unit_group_dwid,
	a.project_dwid,
	lower(p.project_name_text || ' - ' || 'C' || bedroom_count) as unit_group,
	lower(bedroom_count || ' bedroom') as unit_type,
	lower('C' || bedroom_count) as unit_subtype_code,
	gross_floor_area_sqm,
	f_sqm2sqft(gross_floor_area_sqm)::int as gross_floor_area_sqft,
	net_floor_area_sqm,
	f_sqm2sqft(net_floor_area_sqm)::int as net_floor_area_sqft,
	a.unit_count,
	bedroom_count,
	bathroom_count,
	other_room_count,
	--null::int as unit_group_order,
 	--null::boolean as is_penthouse,
 	'hk' as country_code,
 	property_type_code,
 	'insert' as cr_record_action
from unit_group_test a
left join masterdata_hk.project p on a.project_dwid = p.project_dwid 
; -- 43220

update branch_hk.unit_group_cr_627
set unit_group_dwid = api.get_dwid(country_code, 'unit_group', id)
where unit_group_dwid isnull; -- 43220

with base as (
select 
	ug.id,
	trim(trim(replace(replace('project/hk/'||ug.property_type_code||'/'||replace(trim(replace(split_part(ug.unit_group, reverse(split_part(reverse(ug.unit_group), ' - ', 1)), 1), ' - ', ' ')), ' ', '-')
		||'-'||p.id||'/'||ug.bedroom_count||'/'||replace(replace(ug.unit_subtype_code, ' ', '-'), '/', '-')||'-'||coalesce(ug.gross_floor_area_sqm::text, ''),'(',''),')',''), '-')) as slug
from branch_hk.unit_group_cr_627 ug
left join masterdata_hk.project p on ug.project_dwid = p.project_dwid 
)
update branch_hk.unit_group_cr_627 a
set slug = b.slug
from base b where a.id = b.id 
and a.slug isnull
;


insert into branch_hk.property_cr_629
with unit_group_dwidbase as (
select a.property_dwid, b.unit_group_dwid
from masterdata_hk.property a 
left join branch_hk.unit_group_cr_627 b on a.project_dwid = b.project_dwid 
and a.bedroom_count = b.bedroom_count 
and a.gross_floor_area_sqm = b.gross_floor_area_sqm 
and a.net_floor_area_sqm = b.net_floor_area_sqm 
and a.other_room_count = b.other_room_count 
and a.property_type_code = b.property_type_code 
where a.unit_group_dwid isnull and b.unit_group_dwid notnull
)
select 
	p.id,p.property_dwid,p.address_dwid,p.building_dwid,b.unit_group_dwid,p.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join unit_group_dwidbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull; -- 588735

-- should be consistent with:
select sum(unit_count) as cnt from branch_hk.unit_group_cr_627; -- 588735



