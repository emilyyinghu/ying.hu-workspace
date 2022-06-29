select 
	pid,
	usename,
	pg_blocking_pids(pid) as blocked_by,
	query as blocked_query
from pg_stat_activity
where cardinality(pg_blocking_pids(pid)) > 0;


select *
from pg_stat_activity
where pid = 6247

SELECT pg_cancel_backend(4004);

SELECT pg_terminate_backend(4004);


--- consistency check scripts

-- percentage of activity that we have mapped
    
select 
    count(*) as total_records,
    --count(status_code) as status_count,
    count(project_dwid) project_count,
    (count(project_dwid)*100/count(*)) as projects_mapped,
    count(building_dwid),
    (count(building_dwid)*100/count(*)) as buildings_mapped,
    count(address_dwid),
    (count(address_dwid)*100/count(*)) as addresses_mapped,
    count(property_dwid),
    (count(property_dwid)*100/count(*)) as properties_mapped,
    count(activity_dwid),
    (count(activity_dwid)*100/count(*)) as activities_mapped
from map_hk.midland_sale_txn__map mstm  
;
-- 244380	(project)149868	61	(building)207004 84	(address)206982	84	(property)207004 84	(activity)244380 100
--> 244380	150519	61	208251	85	208251	85	208251	85	244380	100
'''need further fix'''


-- query to make sure there is a one to one for 
-- building and address_dwid 
with m as ( 
    select  
        building_dwid
    from 
        masterdata_hk.sale_transaction st 
    where 
        building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid)
)
select count(*) from masterdata_hk.sale_transaction st2 
join m on st2.building_dwid = m.building_dwid;
-- 0  -- 31


--- every building has one parent project
with m as ( 
    select  
        building_dwid
    from 
        masterdata_hk.sale_transaction st 
    where 
        building_dwid notnull 
    group by building_dwid 
    having max(project_dwid) <> min(project_dwid)
)
select count(*) from masterdata_hk.sale_transaction st2 
join m on st2.building_dwid = m.building_dwid;
-- 0 -- 0



-- make sure all transaction properties 
-- match with the properties' underlying details
  
  select 
    count(*)
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.project_dwid <> st.project_dwid;
    -- 1242 -- 0
	'''need further fix'''
    -- all these counts should be zero


  select 
    count(*)
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.building_dwid <> st.building_dwid;
    -- 0 -- 0
   
    
    
  select 
   count(*)
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.address_dwid  <> st.address_dwid;
    -- 0 -- 0 
    

with x as (        
  select 
    p.address_dwid ,
    st.*
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.address_dwid  <> st.address_dwid
)
select 
   count(*)
from 
    "source".hk_midland_realty_sale_transaction s 
join x on s.data_uuid::varchar = x.data_uuid::varchar;
-- 0 -- 0 



--- Make sure sale transaction and the map are in sync one to one    
    
select count(*) from 
    map_hk.midland_sale_txn__map m 
join 
    masterdata_hk.sale_transaction t 
on 
    t.activity_dwid = m.activity_dwid 
where 
    t.property_dwid <> m.property_dwid 
and 
    t.property_dwid notnull 
and 
    m.property_dwid notnull; 
-- 207004 -- 0
'''need further fix'''
-- all these counts should be zero
   
   
select 
    count(*)
    from map_hk.midland_sale_txn__map m 
join 
    masterdata_hk.sale_transaction t 
on 
    t.activity_dwid = m.activity_dwid 
where 
    t.address_dwid  <> m.address_dwid 
and 
    t.address_dwid notnull 
and 
    m.address_dwid notnull; 
-- 0 -- 0 

    
select 
    count(*)
    from map_hk.midland_sale_txn__map m 
join 
    masterdata_hk.sale_transaction t 
on 
    t.activity_dwid = m.activity_dwid 
where 
    t.building_dwid  <> m.building_dwid 
and 
    t.building_dwid notnull 
and 
    m.building_dwid notnull; 
-- 0 -- 0 
    
  
    
select 
    count(*)
    from 
    map_hk.midland_sale_txn__map m 
join 
    masterdata_hk.sale_transaction t 
on 
    t.activity_dwid = m.activity_dwid 
where 
    t.project_dwid  <> m.project_dwid 
and 
    t.project_dwid notnull 
and 
    m.project_dwid notnull; 
-- 0 -- 0
    

--------------------------------------------------
    
-- check that mapping tables and building are in sync 

select 
    b.* 
from masterdata_hk.building b 
    join map_hk.midland_building_to_dwid m on b.building_dwid = m.building_dwid 
where 
    b.project_dwid isnull 
and 
    m.project_dwid notnull
and 
    m.address_dwid = b.address_dwid
; -- 0 records -- 0
        
        
select 
    b.* 
from masterdata_hk.building b 
    join map_hk.midland_building_to_dwid m on b.building_dwid = m.building_dwid 
where 
    b.project_dwid notnull  
and 
    m.project_dwid isnull 
and 
    m.address_dwid = b.address_dwid              
-- 147 records -- 0
'''need further fix'''

    
    

----------
--- find all buildings that have no properties 
--- orphaned buildings

select 
    *
from 
    masterdata_hk.building b 
where 
    not exists (select 1 from masterdata_hk.property p where p.building_dwid = b.building_dwid)
; --1829 -- 1792	
'''need further fix'''




----------------------------------------------

  select 
    *
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.project_dwid <> st.project_dwid
;
    -- 1242  -- 0 
	'''need further fix'''
    -- all these counts should be zero



----------------------------------------------

-- check new address table consistency with old hk_warehouse dm_address table
select count(*) from masterdata_hk.dm_address; --67118
select count(*) from masterdata_hk.dm_address_new_launch; -- 48
select count(*) from masterdata_hk.address; -- 67215

select address_type , address_type_attribute ,count(*)
from masterdata_hk.dm_address group by 1, 2;
'''
locality	city	3
locality	city-area	18
locality	city-subarea	113
locality	metro-area	1
locality	street	200
locality	building	1375
locality	project	2647
point		62761
'''

select address_type_code , count(*) 
from masterdata_hk.address group by 1;
'''
street-address	200
building-address	1375
project-address	2696
point-address	62944
'''

select city , city_area , city_subarea , development , development_phase , address_building , street_name , address_number , count(*)
from masterdata_hk.address
group by 1,2,3,4,5,6,7,8 having count(*) > 1;


select city , city_area , city_subarea , development , development_phase , address_building , street_name  , count(*)
from masterdata_hk.address
group by 1,2,3,4,5,6,7 having count(*) > 1;

with a as (
select development , count(*) as old_cnt
from masterdata_hk.dm_address
group by 1
), b as (
select development , count(*) as new_cnt
from masterdata_hk.address a 
group by 1
)
select *
from a 
join b using (development)
where a.old_cnt <> b.new_cnt


with a as (
select street_name  , count(*) as old_cnt
from masterdata_hk.dm_address
group by 1
), b as (
select street_name , count(*) as new_cnt
from masterdata_hk.address a 
group by 1
)
select *
from a 
join b using (street_name)
where a.old_cnt <> b.new_cnt

-- check new project table consistency with old hk_warehouse dm_project table
select count(*) from masterdata_hk.dm_project ; -- 2519
select count(*) from masterdata_hk.dm_project_new_launch; -- 48
select count(*) from masterdata_hk.project; -- 2568

select dw_address_id notnull, dw_project_id notnull, count(*)
from masterdata_hk.dm_project
group by 1,2;
-- true	true	2519

select address_dwid  notnull, project_dwid  notnull, count(*)
from masterdata_hk.project
group by 1,2;
'''
true		true	2567
false	true	1
--> after fix:
true	true	2567
'''

update masterdata_hk.building 
set project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39'
where project_dwid = '997e214983c4009e3baf560fddee5480'


update masterdata_hk.sale_transaction  
set project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39'
where project_dwid = '997e214983c4009e3baf560fddee5480'


update map_hk.midland_sale_txn__map 
set project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39'
where project_dwid = '997e214983c4009e3baf560fddee5480'


select project_name , count(*)
from masterdata_hk.dm_project group by 1 having count(*) > 1; --49


select project_name  , count(*)
from masterdata_hk.project group by 1 having count(*) > 1; --52

select project_name  , project_type_code , count(*)
from masterdata_hk.project group by 1,2 having count(*) > 1;

-- beach-village, bisney-crest, mount-arcadia, seaside-sonata
-- HK project - address has 1-to-many relationship because 1 project could have multi address street

select project_name  , project_type_code , count(*)
from masterdata_sg.project group by 1,2 having count(*) > 1;


select *
from masterdata_hk.project p
join masterdata_hk.address a on p.dw_address_id = a.dw_address_id 
where p.address_dwid <> a.address_dwid;
-- 108 all new launch projects 


select distinct p.project_dwid , p.address_dwid , p.project_name , p.dw_address_id , p.dw_project_id 
from masterdata_hk.project p
join masterdata_hk.address a on p.dw_address_id = a.dw_address_id 
where p.address_dwid <> a.address_dwid and a.address_type_code = 'project-address' ;
-- 0 records


-- fix HK project - address has 1-to-many relationship --> the project could have multi project-address records in address table, but should be unique in project table
--> 1.mark only one records as 'active' project record and mark others as 'inactive', make sure 'project_dwid' used in all other tables are mapped to the 'active' one
--> 2.delete other 'inactive' records in project table???

select project_name  , project_type_code , location_display_text , count(*)
from masterdata_hk.project group by 1,2,3 having count(*) > 1; --5 

-- beach-village
update map_hk.midland_sale_txn__map 
set project_dwid = '8257d3dad08837ffe904834fb4f0cea4'
where project_dwid = '89f663cd63d7408776b064305ffb0597';

update masterdata_hk.property  
set project_dwid = '8257d3dad08837ffe904834fb4f0cea4'
where project_dwid = '89f663cd63d7408776b064305ffb0597';

update masterdata_hk.sale_transaction  
set project_dwid = '8257d3dad08837ffe904834fb4f0cea4'
where project_dwid = '89f663cd63d7408776b064305ffb0597';

-- celestial-heights
update masterdata_hk.building  
set project_dwid = 'bf96ded3ddf098ae9a7da961ae483211'
where project_dwid = '28cf5ba3b9e847fbf912cc362416c068';

update masterdata_hk.property  
set project_dwid = 'bf96ded3ddf098ae9a7da961ae483211'
where project_dwid = '28cf5ba3b9e847fbf912cc362416c068';

-- mei-foo-sun-chuen
update masterdata_hk.building  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

update masterdata_hk.property  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

update masterdata_hk.sale_transaction  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

update map_hk.midland_sale_txn__map  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

update map_hk.midland_building_to_dwid  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

update map_hk.midland_unit_to_dwid  
set project_dwid = 'c0b0d1e38ed2620df286f0e05dfd1eeb'
where project_dwid = 'cad170ebf413f5284eaa6410969efce0';

-- on-wo-yuen
SELECT x.* FROM masterdata_hk.building x
WHERE project_dwid in ('23ab7c0be7319fd9a16022138bfed95b', '3441fcbc4ab2d59b2f1f0b2a84445bcd')
ORDER BY x.address_display_text,x.project_dwid


-- palm-springs
update masterdata_hk.building  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

update masterdata_hk.property  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

update masterdata_hk.sale_transaction  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

update map_hk.midland_sale_txn__map  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

update map_hk.midland_building_to_dwid  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

update map_hk.midland_unit_to_dwid  
set project_dwid = '5741387ef9965e3352b8f7777aeaab04'
where project_dwid = '571ca10fa23e9b5a962d20d303dc9bb8';

--> check:
select project_name  , project_type_code , location_display_text , count(*)
from masterdata_hk.project where is_active is true
group by 1,2,3 having count(*) > 1; --5 --> 0 



-- check new building table consistency with old hk_warehouse dm_building table
select count(*) from masterdata_hk.dm_building; -- 63370
select count(*) from masterdata_hk.dm_building_new_launch; -- 165
select count(*) from masterdata_hk.building b ; -- 63552

select dw_building_id notnull, dw_address_id notnull, dw_project_id notnull, count(*)
from masterdata_hk.dm_building
group by 1,2,3;
'''
true	true	false	30828
true	true	true	32542
'''

select building_dwid notnull, address_dwid notnull, project_dwid notnull, count(*)
from masterdata_hk.building
group by 1,2,3;
'''
true	true	false	30791
true	true	true	32761
'''
	--- building - address
select *
from masterdata_hk.building p
join masterdata_hk.address a on p.dw_address_id = a.dw_address_id 
where p.address_dwid <> a.address_dwid;
-- all new launch records --> so it's ok??

select distinct p.building_dwid , p.address_dwid , p.project_dwid , p.building_display_name , p.dw_address_id , p.dw_building_id ,p.dw_project_id , a.address_dwid --, a.dw_address_id 
from masterdata_hk.building p
join masterdata_hk.address a on p.dw_address_id = a.dw_address_id 
where p.address_dwid <> a.address_dwid and a.address_type_code = 'point-address' -- 180 
and lower(a.address_building) = lower(p.building_display_name) -- 2
and lower(p.development_phase) = lower(a.development_phase) -- 0
;


-- add buidling - phase mapping feature table for valuation team:
CREATE TABLE feature_hk.de__building__phase_info (
	building_dwid varchar NOT NULL,
	development_phase text NOT NULL,
	CONSTRAINT de__building__phase_info_pk PRIMARY KEY (building_dwid),
	CONSTRAINT de__building__phase_info_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid)
)


insert into feature_hk.de__building__phase_info
(
	building_dwid, development_phase 
)
select distinct building_dwid, development_phase 
from masterdata_hk.building
where development_phase notnull
;


	--- building - project
select *
from masterdata_hk.building p
join masterdata_hk.project a on p.dw_project_id  = a.dw_project_id  
where p.project_dwid  <> a.project_dwid;
-- 5 -- 'beach-village' project 

select distinct development 
from masterdata_hk.building p
join masterdata_hk.project a on p.dw_project_id  = a.dw_project_id  
where p.project_dwid  <> a.project_dwid;
'''-- their id mapping are manually fixed ones so its ok 
palm springs
celestial heights
mei foo sun chuen
on wo yuen
'''

CREATE INDEX ON masterdata_hk.project (address_dwid);
CREATE INDEX ON masterdata_hk.project (dw_project_id);
CREATE INDEX ON masterdata_hk.project (dw_address_id);

CREATE INDEX ON masterdata_hk.building (address_dwid);
CREATE INDEX ON masterdata_hk.building (project_dwid);
CREATE INDEX ON masterdata_hk.building (dw_address_id);
CREATE INDEX ON masterdata_hk.building (dw_project_id);

CREATE INDEX ON masterdata_hk.sale_transaction (address_dwid);
CREATE INDEX ON masterdata_hk.sale_transaction (project_dwid);
CREATE INDEX ON masterdata_hk.sale_transaction (building_dwid);
CREATE INDEX ON masterdata_hk.sale_transaction (property_dwid);
CREATE INDEX ON masterdata_hk.sale_transaction (dw_property_id);

-- CREATE INDEX ON masterdata_hk.property (address_dwid);
-- CREATE INDEX ON masterdata_hk.property (project_dwid);
-- CREATE INDEX ON masterdata_hk.property (dw_address_id);
-- CREATE INDEX ON masterdata_hk.property (dw_project_id);
-- CREATE INDEX ON masterdata_hk.property (building_dwid);
-- CREATE INDEX ON masterdata_hk.property (dw_building_id);
-- CREATE INDEX ON masterdata_hk.property (dw_property_id);
-- CREATE INDEX ON masterdata_hk.property (slug);

CREATE INDEX ON map_hk.midland_building_to_dwid (building_dwid);
CREATE INDEX ON map_hk.midland_building_to_dwid (address_dwid);
CREATE INDEX ON map_hk.midland_building_to_dwid (project_dwid);

CREATE INDEX ON map_hk.midland_unit_to_dwid (building_dwid);
CREATE INDEX ON map_hk.midland_unit_to_dwid (address_dwid);
CREATE INDEX ON map_hk.midland_unit_to_dwid (project_dwid);
CREATE INDEX ON map_hk.midland_unit_to_dwid (property_dwid);

CREATE INDEX ON map_hk.midland_sale_txn__map (building_dwid);
CREATE INDEX ON map_hk.midland_sale_txn__map (address_dwid);
CREATE INDEX ON map_hk.midland_sale_txn__map (project_dwid);
CREATE INDEX ON map_hk.midland_sale_txn__map (property_dwid);
CREATE INDEX ON map_hk.midland_sale_txn__map (activity_dwid);

	--- building - address - project

select distinct address_dwid , project_dwid 
from masterdata_hk.building p;

select *
from masterdata_hk.building p
join masterdata_hk.project a on p.address_dwid  = a.address_dwid  
where p.project_dwid  <> a.project_dwid; -- 0


select project_dwid, a.address_dwid 
from masterdata_hk.building p
left join masterdata_hk.address a on md5(p.development) = md5(a.development)
group by 1,2 order by 1;


with baselist as (
select project_dwid, STRING_AGG(a.address_dwid, ', ') as addressid_list
from masterdata_hk.building p
left join masterdata_hk.address a on md5(p.development) = md5(a.development)
group by 1
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
select *
from masterdata_hk.project a
left join baselist b on a.project_dwid = b.project_dwid
--where position(a.address_dwid in b.addressid_list) > 0
where position(a.address_dwid in b.addressid_list) = 0
; -- to do


-- check new property table consistency with old hk_warehouse dm_property table
select count(*) from masterdata_hk.dm_property; -- 2369667
select count(*) from masterdata_hk.dm_property_new_launch; -- 18415
select count(*) from masterdata_hk.property; -- 2388107

select dw_address_id notnull, dw_project_id notnull, dw_building_id notnull, dw_property_id notnull, count(*)
from masterdata_hk.dm_property
group by 1,2,3,4;
'''
true	false	true	true	692216
true	true	true	true	1677451
'''

select address_dwid notnull, project_dwid notnull, building_dwid notnull, property_dwid notnull, count(*)
from masterdata_hk.property
group by 1,2,3,4;
'''
false	false	false	true	1219
false	true	true	true	8861
true	false	false	true	840
true	false	true	true	692216
true	true	true	true	1684971
'''

select *
from masterdata_hk.property
where address_dwid isnull

select *
from masterdata_hk.property
where slug ilike 'hk/%'

select *
from masterdata_hk.property
where slug ilike 'hk/%' and (original_slug not ilike 'hk/%' or original_slug isnull)


update masterdata_hk.property
set original_slug = slug 
where slug ilike 'hk/%' and original_slug isnull;

update masterdata_hk.property
set slug = NULL
where slug ilike 'hk/%';


UPDATE masterdata_hk.property 
SET slug = trim(trim(replace(replace(replace(replace(regexp_replace(
		(country_code||'/'||property_type_code||'/'||building_dwid||'/'||address_unit||'-'||id)
		, '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-'), '/'))
WHERE slug isnull;


select *
from masterdata_hk.property
where address_dwid isnull; -- 10080

select b.address_dwid , p.*
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
where p.address_dwid isnull and p.building_dwid notnull; --


select *
from masterdata_hk.property
where building_dwid isnull; -- 2,059


-- fix project first, then buidling, finally address
select *
from masterdata_hk.property
where project_dwid isnull; -- 694,275

select *
from masterdata_hk.property
where project_dwid isnull -- 694,275
and split_part(original_slug, '/', 4) != '' --
	--((original_slug ilike 'cn/hk/%' and split_part(original_slug, '/', 4) != '') 
	--or (original_slug ilike 'hk/%' and split_part(original_slug, '/', 4) != ''))
; 

select split_part(original_slug, '/', 1) as slug_prefix, count(*)
from masterdata_hk.property
where project_dwid isnull
and split_part(original_slug, '/', 4) != ''
group by 1
; 
-- cn	28
-- hk	954 --> new launch records

select *
from masterdata_hk.property
where project_dwid isnull -- 694,275
and split_part(original_slug, '/', 4) != '' and split_part(original_slug, '/', 1) = 'cn'
;

select *
from masterdata_hk.project p 
where project_name = 'fairview-park'

select *
from masterdata_hk.building b 
where project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39' and building_name = 'house-51-2nd-street' and lower(development_phase) = replace('section-h', '-',' ')


select p.*, b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39' 
		and b.building_name = split_part(p.original_slug, '/', 6)
		and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) != '' and split_part(p.original_slug, '/', 1) = 'cn'
;


with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39' 
		and b.building_name = split_part(p.original_slug, '/', 6)
		and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) != '' and split_part(p.original_slug, '/', 1) = 'cn'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) != '' and split_part(p.original_slug, '/', 1) = 'cn';

with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = 'ee3e7a21b0d2b4645c94ab2c5ed78c39' 
		and b.building_name = split_part(p.original_slug, '/', 6)
		and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) != '' and split_part(p.original_slug, '/', 1) = 'cn'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'fairview-park' and split_part(p.original_slug, '/', 1) = 'cn';


select *
from masterdata_hk.property
where --project_dwid isnull and 
split_part(original_slug, '/', 4) = 'fairview-park' and split_part(original_slug, '/', 1) = 'cn'
;


-- fix project

select *
from masterdata_hk.property
where project_dwid isnull -- 694,275
and split_part(original_slug, '/', 4) != '' and split_part(original_slug, '/', 1) = 'hk'
;

select *
from masterdata_hk.project p 
where project_name = 'seaside-sonata'


update masterdata_hk.sale_transaction  
set project_dwid = '4f170a028d427f5355ee03cbe46e9e89'
where project_dwid = '6c1a15b8100eceaa5d5d59e922295a52';

update map_hk.midland_sale_txn__map  
set project_dwid = '4f170a028d427f5355ee03cbe46e9e89'
where project_dwid = '6c1a15b8100eceaa5d5d59e922295a52';

update map_hk.midland_unit_to_dwid  
set project_dwid = '4f170a028d427f5355ee03cbe46e9e89'
where project_dwid = '6c1a15b8100eceaa5d5d59e922295a52';


select *
from masterdata_hk.building b 
where project_dwid = '4f170a028d427f5355ee03cbe46e9e89' and building_name = 'tower-5' --and lower(development_phase) = replace('section-h', '-',' ')


select split_part(original_slug, '/', 4), count(*)
from masterdata_hk.property
where project_dwid isnull -- 694,275
and split_part(original_slug, '/', 4) != '' and split_part(original_slug, '/', 1) = 'hk'
group by 1
;

select *
from masterdata_hk.project p 
where project_name = 'bisney-crest'

update masterdata_hk.building  
set project_dwid = '9a98ffa962e8856488c3b8f6c7a7de28'
where project_dwid = '6f32f55b2cdae7968c0db07fc379e1c9';

update masterdata_hk.property  
set project_dwid = '9a98ffa962e8856488c3b8f6c7a7de28'
where project_dwid = '6f32f55b2cdae7968c0db07fc379e1c9';

select *
from masterdata_hk.project p 
where project_name = 'mount-arcadia'

update masterdata_hk.property  
set project_dwid = '52b645aed21f8ed9afa462c6cfe82768'
where project_dwid = '213ffcc03ac2c58d1eac735d70ddb8a8';

update masterdata_hk.sale_transaction  
set project_dwid = '52b645aed21f8ed9afa462c6cfe82768'
where project_dwid = '213ffcc03ac2c58d1eac735d70ddb8a8';

select *
from masterdata_hk.project p 
where project_name = 'university-heights'



select *
from masterdata_hk.building b 
where project_dwid = '4f170a028d427f5355ee03cbe46e9e89' and building_name = 'tower-5' --and lower(development_phase) = replace('section-h', '-',' ')


select p.* , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = '35bc21b4808d9e00fbf2778f640131ab' 
		and b.building_name = split_part(p.original_slug, '/', 5)
		--and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'university-heights' and split_part(p.original_slug, '/', 1) = 'hk'


with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = '35bc21b4808d9e00fbf2778f640131ab' 
		and b.building_name = split_part(p.original_slug, '/', 5)
		--and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'university-heights' and split_part(p.original_slug, '/', 1) = 'hk'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'university-heights' and split_part(p.original_slug, '/', 1) = 'hk';


with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = '4f170a028d427f5355ee03cbe46e9e89' 
		and b.building_name = split_part(p.original_slug, '/', 5)
		--and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'seaside-sonata' and split_part(p.original_slug, '/', 1) = 'hk'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'seaside-sonata' and split_part(p.original_slug, '/', 1) = 'hk';

with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = '52b645aed21f8ed9afa462c6cfe82768' 
		and b.building_name = split_part(p.original_slug, '/', 5)
		--and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'mount-arcadia' and split_part(p.original_slug, '/', 1) = 'hk'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'mount-arcadia' and split_part(p.original_slug, '/', 1) = 'hk';

with base as (
select p.property_dwid , b.building_dwid , b.address_dwid , b.project_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b 
	on b.project_dwid = '9a98ffa962e8856488c3b8f6c7a7de28' 
		and b.building_name = split_part(p.original_slug, '/', 5)
		--and lower(b.development_phase) = replace(split_part(p.original_slug, '/', 5), '-',' ') 
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'bisney-crest' and split_part(p.original_slug, '/', 1) = 'hk'
)
update masterdata_hk.property p
set (project_dwid, building_dwid, address_dwid) = 
(select project_dwid, building_dwid, address_dwid from base where p.property_dwid = base.property_dwid)
where p.project_dwid isnull and split_part(p.original_slug, '/', 4) = 'bisney-crest' and split_part(p.original_slug, '/', 1) = 'hk';



select *
from masterdata_hk.property
where project_dwid isnull 
and split_part(original_slug, '/', 4) != '' and split_part(original_slug, '/', 1) = 'hk'
; -- 0


select *
from masterdata_hk.property
where project_dwid isnull 
and split_part(original_slug, '/', 4) != ''; -- 0 so all the remaining records without project_dwids are indeed do not have project name


-- fix building 
select *
from masterdata_hk.property
where building_dwid isnull; -- 1,077 these records only have address unit without any other info, so just delete them ??

select *
from masterdata_hk.property
where address_unit = '1-b3' and bedroom_count = '2';


DELETE FROM masterdata_hk.property
where building_dwid isnull; -- 1,077


-- fix address
select *
from masterdata_hk.property
where address_dwid isnull; -- 8861

select *
from masterdata_hk.property
where slug isnull and address_unit notnull; -- 951

UPDATE masterdata_hk.property 
SET slug = trim(trim(replace(replace(replace(replace(regexp_replace(
		(country_code||'/'||property_type_code||'/'||building_dwid||'/'||address_unit||'-'||id)
		, '[ ]+', '-'), ' ', '-'), '''', '-'), '---', '-'), '--', '-'), '/'))
WHERE slug isnull;


select b.address_dwid , p.*
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
where p.address_dwid isnull and p.building_dwid notnull;


with base as (
select b.address_dwid as new_address_dwid, p.*
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
where p.address_dwid isnull and p.building_dwid notnull
)
update masterdata_hk.property p
set address_dwid = 
(select new_address_dwid from base where p.property_dwid = base.property_dwid)
where p.address_dwid isnull and p.building_dwid notnull; -- 


select *
from masterdata_hk.property
where address_dwid isnull; -- 8861 --> 0


-- check:

select dw_address_id notnull, dw_project_id notnull, dw_building_id notnull, dw_property_id notnull, count(*)
from masterdata_hk.dm_property
group by 1,2,3,4;
'''
true	false	true	true	692216
true	true	true	true	1677451
'''

select address_dwid notnull, project_dwid notnull, building_dwid notnull, property_dwid notnull, count(*)
from masterdata_hk.property
group by 1,2,3,4;
'''
true	false	true	true	692216
true	true	true	true	1694814
'''




	--- property - address
select p.*, a.*
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid 
left join masterdata_hk.address a on p.dw_address_id = a.dw_address_id and lower(b.building_block_number)  = lower(a.address_building) 
where p.address_dwid  <> a.address_dwid and a.address_type_code = 'point-address';
-- 27,180 --> 0



	--- property - project
select *
from masterdata_hk.property p
join masterdata_hk.project a on p.dw_project_id = a.dw_project_id  
where p.project_dwid <> a.project_dwid;
-- 1,111 --> they are manually updated ones so it's ok


select a.project_name , count(*)
from masterdata_hk.property p
join masterdata_hk.project a on p.dw_project_id = a.dw_project_id  
where p.project_dwid <> a.project_dwid
group by 1;
'''
celestial-heights	949
mei-foo-sun-chuen	5
on-wo-yuen	109
palm-springs	48
'''


	--- property - building
select *
from masterdata_hk.property p
join masterdata_hk.building a on p.dw_building_id = a.dw_building_id  
where p.building_dwid <> a.building_dwid;
-- 0


	--- property - address - project
select *
from masterdata_hk.property p
join masterdata_hk.project a on p.address_dwid  = a.address_dwid  
where p.project_dwid  <> a.project_dwid; 
-- 0

	--- property - address - building
select *
from masterdata_hk.property p
join masterdata_hk.building a on p.address_dwid  = a.address_dwid  
where p.building_dwid  <> a.building_dwid;
-- 0

select *
from masterdata_hk.property p
join masterdata_hk.building a on p.building_dwid  = a.building_dwid  
where p.address_dwid  <> a.address_dwid;
-- 62 , make sense


	--- property - building - project
select p.*, a.*
from masterdata_hk.property p
join masterdata_hk.building a on p.building_dwid = a.building_dwid  
where p.project_dwid <> a.project_dwid; 
-- 1,732 --> 1,626 --> 1517 --> 575 --> 368 --> 0


select pj.project_name  , pj2.project_name, count(*)
from masterdata_hk.property p
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join masterdata_hk.building a on p.building_dwid = a.building_dwid  
left join masterdata_hk.project pj2 on a.project_dwid = pj2.project_dwid 
where p.project_dwid <> a.project_dwid
group by 1,2; 
'''
beach-village	beach-village	1517
on-wo-yuen	on-wo-yuen	215 --> 0
'''

select pj.project_name  , pj2.project_name, a2.development , count(*)
from masterdata_hk.property p
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join masterdata_hk.building a on p.building_dwid = a.building_dwid  
left join masterdata_hk.project pj2 on a.project_dwid = pj2.project_dwid 
left join masterdata_hk.address a2 on p.address_dwid = a2.address_dwid 
where p.project_dwid <> a.project_dwid
group by 1,2,3; 
-- beach-village	beach-village		1517


select p.*, b.*
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid
where p.project_dwid <> b.project_dwid
and b.original_slug not ilike '%seabee%' and b.original_slug not ilike '%seabird%' and b.original_slug not ilike '%seahorse%'
; -- 942 -- they should not have project_dwid because they actually do not have project name -- their original dw_project_id = '6b5a8dec735c2cce4881770849872504'

with base as (
select p.property_dwid 
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid
where p.project_dwid <> b.project_dwid
and b.original_slug not ilike '%seabee%' and b.original_slug not ilike '%seabird%' and b.original_slug not ilike '%seahorse%'
)
update masterdata_hk.property
set project_dwid = NULL
where property_dwid in (select property_dwid from base); --942


with base as (
select p.property_dwid
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid
where p.project_dwid <> b.project_dwid
and b.original_slug ilike '%seabee%' --and b.original_slug not ilike '%seabird%' and b.original_slug not ilike '%seahorse%'
)
update masterdata_hk.property
set project_dwid = '89f663cd63d7408776b064305ffb0597'
where property_dwid in (select property_dwid from base); --207


with base as (
select p.property_dwid
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid
where p.project_dwid <> b.project_dwid
and b.original_slug ilike '%seabird%' --and b.original_slug not ilike '%seahorse%'
)
update masterdata_hk.property
set project_dwid = 'e18b85dc6f05a94f5e059341338a7fec'
where property_dwid in (select property_dwid from base); --208


with base as (
select p.property_dwid
from masterdata_hk.property p
left join masterdata_hk.building b on p.building_dwid = b.building_dwid
where p.project_dwid <> b.project_dwid
and b.original_slug ilike '%seahorse%'
)
update masterdata_hk.property
set project_dwid = '8257d3dad08837ffe904834fb4f0cea4'
where property_dwid in (select property_dwid from base); --160



select address_dwid notnull, project_dwid notnull, building_dwid notnull, property_dwid notnull, count(*)
from masterdata_hk.property
group by 1,2,3,4;
'''
true	false	true	true	693158
true	true	true	true	1693872
'''



	--- property - address - project - building
	--- no need??


-- check new sale_transaction table consistency with old hk_warehouse dm_property_activity table
select count(*) from masterdata_hk.dm_property_activity; -- 1785581
select count(*) from masterdata_hk.dm_property_activity_new_launch; -- 9468
select count(*) from masterdata_hk.sale_transaction; -- 1850872


select f_clone_table('masterdata_hk', 'address', 'masterdata_hk', 'address_backup2', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'building', 'masterdata_hk', 'building_backup2', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'project', 'masterdata_hk', 'project_backup2', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'property', 'masterdata_hk', 'property_backup2', TRUE, TRUE);
select f_clone_table('masterdata_hk', 'sale_transaction', 'masterdata_hk', 'sale_transaction_backup2', TRUE, TRUE);


	--- property - address - project - building id relationship is same as the ones in property table

select 
	p.address_dwid = st.address_dwid as addressid_consistency,
	p.project_dwid = st.project_dwid as projectid_consistency, 
	p.building_dwid = st.building_dwid as buildingid_consistency,
	count(*)
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull
group by 1,2,3;
'''
false	true	true	113 --> fixed
true	false	true	78 --> fixed
true	true	true	1392350 --> [ our target ]
true	null	true	206530 --> should be NULL at the same time --> fixed
null	false	true	11 --> fixed
null	true	true	2230 --> fixed
null	true	null	1076 --> fixed
null	null	true	13 --> fixed
--	null	null	null	248471
'''

select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.project_dwid = st.project_dwid 
and p.address_dwid <> st.address_dwid;


update masterdata_hk.address 
set address_building = lower(address_building)
where address_building notnull;
-- other updated columns: address_street_text, street_name, street_name_root


with base as (
select st.activity_dwid , p.address_dwid 
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.project_dwid = st.project_dwid 
and p.address_dwid <> st.address_dwid
)
update masterdata_hk.sale_transaction st
set address_dwid = 
(select address_dwid from base where st.activity_dwid = base.activity_dwid)
where activity_dwid in (select activity_dwid from base); -- 113

select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and p.project_dwid  <> st.project_dwid;

with base as (
select st.activity_dwid , p.project_dwid
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and p.project_dwid  <> st.project_dwid
)
update masterdata_hk.sale_transaction st
set project_dwid = 
(select project_dwid from base where st.activity_dwid = base.activity_dwid)
where activity_dwid in (select activity_dwid from base); -- 78


select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and (p.project_dwid isnull or st.project_dwid isnull); -- 206530


select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and p.project_dwid isnull and st.project_dwid notnull; -- 421

select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and p.project_dwid notnull and st.project_dwid isnull; -- 0


with base as (
select st.activity_dwid 
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid and p.address_dwid = st.address_dwid 
and p.project_dwid isnull and st.project_dwid notnull
)
update masterdata_hk.sale_transaction
set project_dwid = NULL
where activity_dwid in (select activity_dwid from base); -- 421

select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid 
and p.project_dwid  <> st.project_dwid
and (p.address_dwid isnull or st.address_dwid isnull); -- 11


select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid 
and p.project_dwid  = st.project_dwid
and (p.address_dwid isnull or st.address_dwid isnull); -- 2230


with base as (
select st.activity_dwid, p.address_dwid 
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull and p.building_dwid = st.building_dwid 
and p.project_dwid  = st.project_dwid
and p.address_dwid notnull and st.address_dwid isnull
)
update masterdata_hk.sale_transaction st
set address_dwid = 
(select address_dwid from base where st.activity_dwid = base.activity_dwid)
where activity_dwid in (select activity_dwid from base); -- 2230



select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull
and p.project_dwid  = st.project_dwid
and (p.address_dwid isnull or st.address_dwid isnull)
and (p.building_dwid isnull or st.building_dwid isnull); -- 1076


with base as (
select st.activity_dwid, p.address_dwid , p.building_dwid 
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull
and p.project_dwid  = st.project_dwid
and (p.address_dwid isnull or st.address_dwid isnull)
and (p.building_dwid isnull or st.building_dwid isnull)
)
update masterdata_hk.sale_transaction st
set (address_dwid, building_dwid) = 
(select address_dwid, building_dwid from base where st.activity_dwid = base.activity_dwid)
where activity_dwid in (select activity_dwid from base); -- 1076


select p.*, st.*
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull
and p.building_dwid  = st.building_dwid
and (p.address_dwid isnull or st.address_dwid isnull)
and (p.project_dwid isnull or st.project_dwid isnull); -- 13


-- check:
select 
	p.address_dwid = st.address_dwid as addressid_consistency,
	p.project_dwid = st.project_dwid as projectid_consistency, 
	p.building_dwid = st.building_dwid as buildingid_consistency,
	count(*)
from masterdata_hk.sale_transaction st
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where st.property_dwid notnull
group by 1,2,3;
'''
true	true	true	1395871
true		true	206530
'''



	-- to do: check id distribution
select dw_property_id notnull, dw_activity_id notnull, count(*)
from masterdata_hk.dm_property_activity dpa 
group by 1,2;
'''
false	true	247218
true	true	1538363
'''

select address_dwid notnull, project_dwid notnull, building_dwid notnull, property_dwid notnull, activity_dwid notnull, count(*)
from masterdata_hk.sale_transaction st 
group by 1,2,3,4,5;
'''
false	false	false	false	true	248438
false	false	true	false	true	4
false	false	true	true	true	13
false	true	false	false	true	29
false	true	false	true	true	1076
false	true	true	true	true	2241
true	false	true	true	true	206109
true	true	true	true	true	1392962
'''


select address_dwid notnull, project_dwid notnull, building_dwid notnull, count(*)
from masterdata_hk.sale_transaction st 
where property_dwid notnull
group by 1,2,3;
'''
false	false	true	13
false	true	false	1076
false	true	true	2241
true	false	true	206109
true	true	true	1392962

--> 
true	false	true	206530
true	true	true	1395871

make sense
'''


-- to do: update id relationship in map tables 
	-- midland_building_to_dwid
	-- midland_sale_txn__map
	-- midland_unit_to_dwid

select f_clone_table('map_hk', 'midland_sale_txn__map', 'map_hk', 'midland_sale_txn__map_backup2', TRUE, TRUE);
select f_clone_table('map_hk', 'midland_building_to_dwid', 'map_hk', 'midland_building_to_dwid_backup2', TRUE, TRUE);
select f_clone_table('map_hk', 'midland_unit_to_dwid', 'map_hk', 'midland_unit_to_dwid_backup2', TRUE, TRUE);


update map_hk.midland_sale_txn__map mp
set (property_dwid, building_dwid, address_dwid, project_dwid) = 
(select property_dwid, building_dwid, address_dwid, project_dwid 
from masterdata_hk.sale_transaction st 
where mp.activity_dwid = st.activity_dwid)
where mp.activity_dwid notnull;

update map_hk.midland_unit_to_dwid u
set (building_dwid, address_dwid, project_dwid) = 
(select building_dwid, address_dwid, project_dwid 
from masterdata_hk.property p 
where u.property_dwid = p.property_dwid)
where u.property_dwid notnull;

with idbase as (
select building_id , building_dwid, address_dwid, project_dwid, 
	ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
from map_hk.midland_unit_to_dwid
group by 1,2,3,4
)
, base as (
select building_id , building_dwid, address_dwid, project_dwid
from idbase where seq = 1
)
update map_hk.midland_building_to_dwid b
set (building_dwid, address_dwid, project_dwid) = 
(select building_dwid, address_dwid, project_dwid 
from base where b.building_id = base.building_id
)
where b.building_id notnull;


-- check map tables id consistency: see the queries provided above

	-- only this one needs fix:
	-- query to make sure there is a one to one for building and address_dwid 
with m as ( 
    select  
        building_dwid
    from 
        masterdata_hk.sale_transaction st 
    where 
        building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid)
)
select count(*) from masterdata_hk.sale_transaction st2 
join m on st2.building_dwid = m.building_dwid;
-- 31

with m as ( 
    select building_dwid
    from masterdata_hk.sale_transaction st 
    where building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid) -- b32bb9ace217e74d0aaa49fccd4c9690
)
select count(*) from masterdata_hk.sale_transaction st2 
join m on st2.building_dwid = m.building_dwid;
-- 0



-- replicate updated masterdata_hk and feature_hk, map_hk into redshift










