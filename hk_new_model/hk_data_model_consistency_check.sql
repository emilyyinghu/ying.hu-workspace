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
-- 0


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
-- 0



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
    -- 1242 --> need further fix
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
    -- 0
   
    
    
  select 
   count(*)
  from masterdata_hk.sale_transaction st 
  left join 
    masterdata_hk.property p 
  on 
    p.property_dwid = st.property_dwid 
where 
    p.address_dwid  <> st.address_dwid;
    -- 0
    

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
join x on s.data_uuid::varchar = x.data_uuid;
-- 0

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
-- need further fix
    
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
-- 0

    
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
-- 0
    
  
    
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
-- 0
    
    
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
            ; -- 0 records
        
        
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
        -- 147 records --> need further fix

    ----------
    --- find all buildings that have no properties 
    --- orphaned buildings

    select 
        *
    from 
        masterdata_hk.building b 
    where 
        not exists (select 1 from masterdata_hk.property p where p.building_dwid = b.building_dwid)
    ; --1829 need further fix





