--- healthchecks for dwid relationships among masterdata_hk, map_hk and premap_hk
'''
1.rent_transaction v.s. midland_rent_txn_map
2.rent_transaction v.s. midland_unit_to_dwid
3.rent_transaction v.s. midland_building_to_dwid️
4.midland_rent_txn_map v.s. midland_unit_to_dwid
5.midland_unit_to_dwid v.s. midland_building_to_dwid
'''

--1.rent_transaction v.s. midland_rent_txn_map

select a.*
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0

--2.rent_transaction v.s. midland_unit_to_dwid

select a.*, c.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
; -- <= 3 < 10 (some special edgecases) -- 0


--3.rent_transaction v.s. midland_building_to_dwid️

select a.*, c.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
; -- 22


--4.midland_rent_txn_map v.s. midland_unit_to_dwid
select a.*, c.*
from map_hk.midland_rent_txn__map a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
; --  <= 3 < 10 (some special edgecases) -- 0


--5.midland_unit_to_dwid v.s. midland_building_to_dwid
with idbase as (
	select building_id , building_dwid, address_dwid, project_dwid, 
		ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
	from premap_hk.midland_unit_to_dwid
	group by 1,2,3,4
)
, base as (
	select building_id , building_dwid, address_dwid, project_dwid
	from idbase where seq = 1
)
select a.*, b.*
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0


--------------------------------------------------------------------------------------------

--1.rent_listing v.s. midland_rent_listing_map
select a.*
from masterdata_hk.rent_listing a
left join map_hk.midland_rent_listing__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0


--2.rent_listing v.s. midland_unit_to_dwid
select a.*, c.*
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
	and b.unit_id notnull --> 0
; -- 458 to do



--3.rent_listing v.s. midland_building_to_dwid
select a.*, c.*
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
; -- 4

--4.midland_rent_listing_map v.s. midland_unit_to_dwid
select a.*, c.*
from map_hk.midland_rent_listing__map a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
; -- < 10 (some special edgecases)


--5.midland_unit_to_dwid v.s. midland_building_to_dwid
with idbase as (
select building_id , building_dwid, address_dwid, project_dwid, 
	ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
from premap_hk.midland_unit_to_dwid
group by 1,2,3,4
)
, base as (
select building_id , building_dwid, address_dwid, project_dwid
from idbase where seq = 1
)
select a.*, b.*
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and a.building_id notnull
; -- 0

-- need to insert missing records into midland_unit_to_dwid
with idbase as (
select building_id , building_dwid, address_dwid, project_dwid, 
	ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
from premap_hk.midland_unit_to_dwid
group by 1,2,3,4
)
, base as (
select building_id , building_dwid, address_dwid, project_dwid
from idbase where seq = 1
)
select a.*, b.*
from premap_hk.midland_building_to_dwid a
right join base b on a.building_id = b.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
;



--------------------------------------------------------------------------------------------

--1.sale_listing v.s. midland_sale_listing_map
select a.*, b.*
from masterdata_hk.sale_listing a
left join map_hk.midland_sale_listing__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
and data_source = 'hk-midland-listing-sale'
; -- 0

--2.sale_listing v.s. midland_unit_to_dwid
select a.*, c.*
from masterdata_hk.sale_listing  a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
and data_source = 'hk-midland-listing-sale'
; -- 

--3.sale_listing v.s. midland_building_to_dwid
select a.*, c.*
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
and data_source = 'hk-midland-listing-sale'
; -- 0




--4.midland_sale_listing_map v.s. midland_unit_to_dwid
select a.*, c.*
from map_hk.midland_sale_listing__map a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
--and data_source = 'hk-midland-listing-sale'
; -- < 10 (some special edgecases)



--5.midland_unit_to_dwid v.s. midland_building_to_dwid
with idbase as (
	select building_id , building_dwid, address_dwid, project_dwid, 
		ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
	from premap_hk.midland_unit_to_dwid
	group by 1,2,3,4
)
, base as (
	select building_id , building_dwid, address_dwid, project_dwid
	from idbase where seq = 1
)
select a.*, b.*
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0



--------------------------------------------------------------------------------------------

--- consistency check

--1.each masterdata_hk core table v.s. map_hk map table - using data_uuid

--rent_transaction v.s. midland_rent_txn_map

select a.*
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0

-- rent_listing v.s. midland_rent_listing_map
select a.*
from masterdata_hk.rent_listing a
left join map_hk.midland_rent_listing__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0


-- sale_listing v.s. midland_sale_listing_map
select a.*, b.*
from masterdata_hk.sale_listing a
left join map_hk.midland_sale_listing__map b on a.data_uuid = b.data_uuid 
where md5(
		f_prep_dw_id(a.activity_dwid)||'__'||
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.activity_dwid)||'__'||
		f_prep_dw_id(b.property_dwid)||'__'||
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
and data_source = 'hk-midland-listing-sale'
; -- 0



--2.union masterdata_hk core table v.s. premap_hk.midland_unit_to_dwid - using unit_id, use core table to update premap table
with unit_base as (
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id , 'rent_transaction' as source_table 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
--44,747
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'rent_listing' as source_table 
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
--64,671
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'sale_listing' as source_table 
from masterdata_hk.sale_listing  a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
where a.data_source = 'hk-midland-listing-sale'
-- 94,558
)-- 203,976
select 
(a.property_dwid notnull)::int+(a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int as score_a,
(c.property_dwid notnull)::int+(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int as score_c,
a.* , c.*
from unit_base a
left join premap_hk.midland_unit_to_dwid c on a.unit_id = c.unit_id
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) -- 2144
and  a.unit_id notnull and c.unit_id notnull -- 321 --> 27 --> 24
order by 2 desc, 1 desc
;


-- update
with unit_base as (
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id , 'rent_transaction' as source_table 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
--44,747
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'rent_listing' as source_table 
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
--64,671
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'sale_listing' as source_table 
from masterdata_hk.sale_listing  a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
where a.data_source = 'hk-midland-listing-sale'
-- 94,558
)-- 203,976
, update_unit_map_base as (
select 
(a.property_dwid notnull)::int+(a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int as score_a,
(c.property_dwid notnull)::int+(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int as score_c,
a.* --, c.*
from unit_base a
left join premap_hk.midland_unit_to_dwid c on a.unit_id = c.unit_id
where md5(
		f_prep_dw_id(a.property_dwid)||'__'||
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.property_dwid)||'__'||
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) -- 2144
and  a.unit_id notnull and c.unit_id notnull -- 321
order by 2 desc, 1 desc
)
, update_unit_map as (
select distinct unit_id, property_dwid , building_dwid , address_dwid , project_dwid 
from update_unit_map_base
)
update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from update_unit_map b
where a.unit_id = b.unit_id 
; -- 291 + 20



--3.union masterdata_hk core table v.s. premap_hk.midland_building_to_dwid - using building_id, use core table to update premap table


with unit_base as (
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id , 'rent_transaction' as source_table 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
--44,747
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'rent_listing' as source_table 
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
--64,671
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'sale_listing' as source_table 
from masterdata_hk.sale_listing  a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
where a.data_source = 'hk-midland-listing-sale'
-- 94,558
)-- 203,976
select 
(a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int as score_a,
(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int as score_c,
a.* , c.*
from unit_base a
left join premap_hk.midland_building_to_dwid c on a.building_id = c.building_id 
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) -- 113 --> 31
--and a.building_id notnull and c.building_id notnull
order by 2 desc, 1 desc
;

-- update 

with unit_base as (
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id , 'rent_transaction' as source_table 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
--44,747
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'rent_listing' as source_table 
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
--64,671
union 
select a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , a.data_uuid , b.building_id, b.unit_id  , 'sale_listing' as source_table 
from masterdata_hk.sale_listing  a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
where a.data_source = 'hk-midland-listing-sale'
-- 94,558
)-- 203,976
, update_building_map_base as (
select 
(a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int as score_a,
(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int as score_c,
a.* --, c.*
from unit_base a
left join premap_hk.midland_building_to_dwid c on a.building_id = c.building_id 
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) -- 113
--and a.building_id notnull and c.building_id notnull
order by 2 desc, 1 desc
)
--, update_building_map as (
select distinct building_id , building_dwid , address_dwid , project_dwid 
from update_building_map_base -- 18
)
update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from update_building_map b
where a.building_id = b.building_id 
; -- 12



-- 4.premap_hk.midland_unit_to_dwid v.s. premap_hk.midland_building_to_dwid - using building_id

with idbase as (
	select building_id , building_dwid, address_dwid, project_dwid, 
		ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
	from premap_hk.midland_unit_to_dwid
	group by 1,2,3,4
)
, base as (
	select building_id , building_dwid, address_dwid, project_dwid
	from idbase where seq = 1
)
select a.*, b.*
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(b.address_dwid)||'__'||
		f_prep_dw_id(b.building_dwid)||'__'||
		f_prep_dw_id(b.project_dwid)
		) 
; -- 0



---- DONE:
ALTER TABLE map_hk.midland_rent_txn__map ADD CONSTRAINT midland_rent_txn__map_fk_activity_dwid FOREIGN KEY (activity_dwid) REFERENCES masterdata_hk.rent_transaction(activity_dwid);
ALTER TABLE map_hk.midland_rent_txn__map ADD CONSTRAINT midland_rent_txn__map_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE map_hk.midland_rent_txn__map ADD CONSTRAINT midland_rent_txn__map_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE map_hk.midland_rent_txn__map ADD CONSTRAINT midland_rent_txn__map_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE map_hk.midland_rent_txn__map ADD CONSTRAINT midland_rent_txn__map_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
CREATE INDEX midland_rent_txn__map_activity_dwid_idx ON map_hk.midland_rent_txn__map USING btree (activity_dwid);
CREATE INDEX midland_rent_txn__map_address_dwid_idx ON map_hk.midland_rent_txn__map USING btree (address_dwid);
CREATE INDEX midland_rent_txn__map_building_dwid_idx ON map_hk.midland_rent_txn__map USING btree (building_dwid);
CREATE INDEX midland_rent_txn__map_project_dwid_idx ON map_hk.midland_rent_txn__map USING btree (project_dwid);
CREATE INDEX midland_rent_txn__map_property_dwid_idx ON map_hk.midland_rent_txn__map USING btree (property_dwid);


ALTER TABLE map_hk.midland_rent_listing__map ADD CONSTRAINT midland_rent_listing__map_fk_activity_dwid FOREIGN KEY (activity_dwid) REFERENCES masterdata_hk.rent_listing(activity_dwid);
ALTER TABLE map_hk.midland_rent_listing__map ADD CONSTRAINT midland_rent_listing__map_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE map_hk.midland_rent_listing__map ADD CONSTRAINT midland_rent_listing__map_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE map_hk.midland_rent_listing__map ADD CONSTRAINT midland_rent_listing__map_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE map_hk.midland_rent_listing__map ADD CONSTRAINT midland_rent_listing__map_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
CREATE INDEX midland_rent_listing__map_activity_dwid_idx ON map_hk.midland_rent_listing__map USING btree (activity_dwid);
CREATE INDEX midland_rent_listing__map_address_dwid_idx ON map_hk.midland_rent_listing__map USING btree (address_dwid);
CREATE INDEX midland_rent_listing__map_building_dwid_idx ON map_hk.midland_rent_listing__map USING btree (building_dwid);
CREATE INDEX midland_rent_listing__map_project_dwid_idx ON map_hk.midland_rent_listing__map USING btree (project_dwid);
CREATE INDEX midland_rent_listing__map_property_dwid_idx ON map_hk.midland_rent_listing__map USING btree (property_dwid);


ALTER TABLE map_hk.midland_sale_listing__map ADD CONSTRAINT midland_sale_listing__map_fk_activity_dwid FOREIGN KEY (activity_dwid) REFERENCES masterdata_hk.sale_listing(activity_dwid);
ALTER TABLE map_hk.midland_sale_listing__map ADD CONSTRAINT midland_sale_listing__map_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE map_hk.midland_sale_listing__map ADD CONSTRAINT midland_sale_listing__map_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE map_hk.midland_sale_listing__map ADD CONSTRAINT midland_sale_listing__map_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE map_hk.midland_sale_listing__map ADD CONSTRAINT midland_sale_listing__map_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
CREATE INDEX midland_sale_listing__map_activity_dwid_idx ON map_hk.midland_sale_listing__map USING btree (activity_dwid);
CREATE INDEX midland_sale_listing__map_address_dwid_idx ON map_hk.midland_sale_listing__map USING btree (address_dwid);
CREATE INDEX midland_sale_listing__map_building_dwid_idx ON map_hk.midland_sale_listing__map USING btree (building_dwid);
CREATE INDEX midland_sale_listing__map_project_dwid_idx ON map_hk.midland_sale_listing__map USING btree (project_dwid);
CREATE INDEX midland_sale_listing__map_property_dwid_idx ON map_hk.midland_sale_listing__map USING btree (property_dwid);


--------------------------------------------------------------------------------------------
select a.data_uuid from map_hk.midland_sale_txn__map a
where a.activity_dwid isnull; -- 255

select a.data_uuid 
from map_hk.midland_sale_txn__map a
left join "source".hk_midland_realty_sale_transaction b on a.data_uuid = b.data_uuid 
where a.activity_dwid isnull and b.data_uuid isnull; -- 255

delete from map_hk.midland_sale_txn__map
where activity_dwid isnull; -- 255

select b.data_uuid notnull, count(*)
from map_hk.midland_sale_listing__map a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
where a.activity_dwid isnull
group by 1; -- true		588

delete from map_hk.midland_sale_listing__map
where activity_dwid isnull; -- 588

select b.data_uuid notnull, count(*)
from map_hk.midland_rent_listing__map a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
where a.activity_dwid isnull
group by 1; -- true	833

delete from map_hk.midland_rent_listing__map
where activity_dwid isnull; -- 833



--- consistency check with core tables

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
from map_hk.midland_rent_txn__map mrtm 
;
-- 44747	(project)37686	84	(building)41500	92	(address)41721	93	(property)38207	85	(activity)44747	100

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
from map_hk.midland_sale_listing__map mslm 
;
-- 94558	(project)73264	77	(building)85132	90	(address)85519	90	(property)76470	80	(activity)94558	100


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
from map_hk.midland_rent_listing__map mrlm 
;
-- 64671	(project)49494	76	(building)57886	89	(address)58159	89	(property)51497	79	(activity)64671	100



-- query to make sure there is a one to one for 
-- every building has one address_dwid 
with m as ( 
    select building_dwid
    from masterdata_hk.rent_transaction st 
    where building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid)
)
select count(*) from masterdata_hk.rent_transaction st2 
join m on st2.building_dwid = m.building_dwid;
--0

with m as ( 
    select building_dwid
    from masterdata_hk.sale_listing st 
    where building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid)
)
select count(*) from masterdata_hk.sale_listing st2 
join m on st2.building_dwid = m.building_dwid;
-- 0

with m as ( 
    select building_dwid
    from masterdata_hk.rent_listing st 
    where building_dwid notnull 
    group by building_dwid 
    having max(address_dwid) <> min(address_dwid)
)
select count(*) from masterdata_hk.rent_listing st2 
join m on st2.building_dwid = m.building_dwid;
-- 0


--- every building has one parent project
with m as ( 
    select building_dwid
    from masterdata_hk.rent_transaction st 
    where building_dwid notnull 
    group by building_dwid 
    having max(project_dwid) <> min(project_dwid)
)
select count(*) from masterdata_hk.rent_transaction st2 
join m on st2.building_dwid = m.building_dwid;
-- 0

with m as ( 
    select building_dwid
    from masterdata_hk.sale_listing st 
    where building_dwid notnull 
    group by building_dwid 
    having max(project_dwid) <> min(project_dwid)
)
select count(*) from masterdata_hk.sale_listing st2 
join m on st2.building_dwid = m.building_dwid;
-- 0

with m as ( 
    select building_dwid
    from masterdata_hk.rent_listing st 
    where building_dwid notnull 
    group by building_dwid 
    having max(project_dwid) <> min(project_dwid)
)
select count(*) from masterdata_hk.rent_listing st2 
join m on st2.building_dwid = m.building_dwid;
-- 0


-- make sure all transaction properties 
-- match with the properties' underlying details
  
select count(*)
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0
select count(*)
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0
select count(*)
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0

'''DONE'''

select count(*)
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; --28 -- 0 --- DONE
select count(*)
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; -- 138 -- 0 --- DONE
select count(*)
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; -- 73 -- 0 --- DONE

'''DONE''' -- same with buildidng_dwids

select count(*)
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.address_dwid  <> st.address_dwid;-- 28 -- 0 --- DONE
select count(*)
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.address_dwid  <> st.address_dwid;-- 138 -- 0 --- DONE
select count(*)
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.address_dwid  <> st.address_dwid;-- 73 -- 0 --- DONE

'''DONE''' -- same with buildidng_dwids

with x as (        
  	select p.address_dwid , st.*
  	from masterdata_hk.rent_transaction st 
  	left join masterdata_hk.property p 
  	on p.property_dwid = st.property_dwid 
	where p.address_dwid  <> st.address_dwid
)
select count(*)
from "source".hk_midland_realty_rental_transaction s 
join x on s.data_uuid::varchar = x.data_uuid::varchar;-- 28 -- 0 --- DONE

with x as (        
  	select p.address_dwid , st.*
  	from masterdata_hk.sale_listing st 
  	left join masterdata_hk.property p 
  	on p.property_dwid = st.property_dwid 
	where p.address_dwid  <> st.address_dwid
)
select count(*)
from "source".hk_midland_realty_sale_listing s 
join x on s.data_uuid::varchar = x.data_uuid::varchar;-- 138 -- 0 --- DONE

with x as (        
  	select p.address_dwid , st.*
  	from masterdata_hk.rent_listing st 
  	left join masterdata_hk.property p 
  	on p.property_dwid = st.property_dwid 
	where p.address_dwid  <> st.address_dwid
)
select count(*)
from "source".hk_midland_realty_rental_listing s 
join x on s.data_uuid::varchar = x.data_uuid::varchar;-- 73 -- 0 --- DONE



--- Make sure sale transaction and the map are in sync one to one    
select count(*) 
from map_hk.midland_rent_txn__map  m 
join masterdata_hk.rent_transaction t 
on t.activity_dwid = m.activity_dwid 
where t.property_dwid <> m.property_dwid 
and t.property_dwid notnull and m.property_dwid notnull; -- 0
select count(*) 
from map_hk.midland_sale_listing__map m 
join masterdata_hk.sale_listing t 
on t.activity_dwid = m.activity_dwid 
where t.property_dwid <> m.property_dwid 
and t.property_dwid notnull and m.property_dwid notnull; -- 0
 select count(*) 
from map_hk.midland_rent_listing__map m 
join masterdata_hk.rent_listing t 
on t.activity_dwid = m.activity_dwid 
where t.property_dwid <> m.property_dwid 
and t.property_dwid notnull and m.property_dwid notnull; -- 0
 

select count(*) 
from map_hk.midland_rent_txn__map  m 
join masterdata_hk.rent_transaction t 
on t.activity_dwid = m.activity_dwid 
where t.address_dwid <> m.address_dwid 
and t.address_dwid notnull and m.address_dwid notnull; -- 0
select count(*) 
from map_hk.midland_sale_listing__map m 
join masterdata_hk.sale_listing t 
on t.activity_dwid = m.activity_dwid 
where t.address_dwid <> m.address_dwid 
and t.address_dwid notnull and m.address_dwid notnull; -- 0
select count(*) 
from map_hk.midland_rent_listing__map m 
join masterdata_hk.rent_listing t 
on t.activity_dwid = m.activity_dwid 
where t.address_dwid <> m.address_dwid 
and t.address_dwid notnull and m.address_dwid notnull; -- 0
 

select count(*) 
from map_hk.midland_rent_txn__map  m 
join masterdata_hk.rent_transaction t 
on t.activity_dwid = m.activity_dwid 
where t.building_dwid <> m.building_dwid 
and t.building_dwid notnull and m.building_dwid notnull; -- 0
select count(*) 
from map_hk.midland_sale_listing__map m 
join masterdata_hk.sale_listing t 
on t.activity_dwid = m.activity_dwid 
where t.building_dwid <> m.building_dwid 
and t.building_dwid notnull and m.building_dwid notnull; -- 0
select count(*) 
from map_hk.midland_rent_listing__map m 
join masterdata_hk.rent_listing t 
on t.activity_dwid = m.activity_dwid 
where t.building_dwid <> m.building_dwid 
and t.building_dwid notnull and m.building_dwid notnull; -- 0
 

select count(*) 
from map_hk.midland_rent_txn__map  m 
join masterdata_hk.rent_transaction t 
on t.activity_dwid = m.activity_dwid 
where t.project_dwid <> m.project_dwid 
and t.project_dwid notnull and m.project_dwid notnull; -- 0
select count(*) 
from map_hk.midland_sale_listing__map m 
join masterdata_hk.sale_listing t 
on t.activity_dwid = m.activity_dwid 
where t.project_dwid <> m.project_dwid 
and t.project_dwid notnull and m.project_dwid notnull; -- 0
select count(*) 
from map_hk.midland_rent_listing__map m 
join masterdata_hk.rent_listing t 
on t.activity_dwid = m.activity_dwid 
where t.project_dwid <> m.project_dwid 
and t.project_dwid notnull and m.project_dwid notnull; -- 0
 

-- check that mapping tables and building are in sync 

select b.* 
from masterdata_hk.building b 
join premap_hk.midland_building_to_dwid m on b.building_dwid = m.building_dwid 
where b.project_dwid isnull 
and m.project_dwid notnull
and m.address_dwid = b.address_dwid; -- 0
  
select b.* 
from masterdata_hk.building b 
join premap_hk.midland_building_to_dwid m on b.building_dwid = m.building_dwid 
where b.project_dwid notnull  
and m.project_dwid isnull 
and m.address_dwid = b.address_dwid; -- 0
   

--- find all buildings that have no properties 
--- orphaned buildings
'''TO DO''' 

select b.* from masterdata_hk.building b 
where not exists (select 1 from masterdata_hk.property p where p.building_dwid = b.building_dwid)
; -- 1715 -- the same count compared with before, so we do not need to do fix now.
   
   
---
select *
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0
select *
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0
select *
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.project_dwid <> st.project_dwid; -- 0  



