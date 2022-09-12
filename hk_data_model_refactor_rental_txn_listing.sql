-- rental_transaction
DROP TABLE masterdata_hk.rent_transaction;

CREATE TABLE masterdata_hk.rent_transaction (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_name text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	rent_start_date date NULL,
	rent_end_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	CONSTRAINT rent_transaction_pk PRIMARY KEY (id),
	CONSTRAINT rent_transaction_un_dwid UNIQUE (activity_dwid)
);
CREATE INDEX rent_transaction_data_uuid_idx ON masterdata_hk.rent_transaction USING btree (data_uuid);


DROP TABLE masterdata_hk.rent_transaction_w_old_dw_ids;

CREATE TABLE masterdata_hk.rent_transaction_w_old_dw_ids (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_name text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	rent_start_date date NULL,
	rent_end_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	dw_property_id text NULL,
	original_slug text NULL
);


--- use reference/source tables to create from the beginning and then join the ids rather than use dm tables


create table playground.hk_rental_transaction_temp as
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill_w_bedroom
    where unit_id notnull and floor notnull
)
--, midland_rental_txn_base as (
    select
        initcap(b.region_name) as city,
        initcap(b.subregion_name) as city_area,
        initcap(b.district_name) as city_subarea,
        b.corrected_estate_name,
        b.corrected_phase_name,
        b.corrected_building_name,
        b.corrected_street_num,
        b.corrected_street_name,
        mu.floor,
        a.*,
        c.dw_address_id,
        c.slug,
        c.fingerprint,
        c.full_address_text,
        c.latitude,
        c.longitude,
        c.postal_code,
        d.dw_property_id,
        row_number() over(partition by id) as rn
    from source.hk_midland_realty_rental_transaction a
        left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
        left join midland_units mu on a.unit_id = mu.unit_id and mu.rn = 1
        left join raw_internal.address c
            on f_prep_dw_id(b.region_name) = f_prep_dw_id(c.city)
            and f_prep_dw_id(b.subregion_name) = f_prep_dw_id(c.city_area)
            and f_prep_dw_id(b.district_name) = f_prep_dw_id(c.city_subarea)
            and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
            and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
            and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
            and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
            and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name)
        left join masterdata_hk.z_dm_property d
            on d.dw_address_id = c.dw_address_id
--             and f_prep_dw_id(d.city) = f_prep_dw_id(b.region_name)
--             and f_prep_dw_id(d.city_area) = f_prep_dw_id(b.subregion_name)
--             and f_prep_dw_id(d.city_subarea) = f_prep_dw_id(b.district_name)
--             and f_prep_dw_id(d.project_name)= f_prep_dw_id(b.corrected_estate_name)
--             and f_prep_dw_id(c.development_phase)= f_prep_dw_id(b.corrected_phase_name)
--             and f_prep_dw_id(d.building_name) = f_prep_dw_id(b.corrected_building_name)
            and f_prep_dw_id(d.address_floor) = f_prep_dw_id(mu.floor)
            and f_prep_dw_id(d.address_stack) = f_prep_dw_id(a.flat)
; -- 47766


with base as (
select 
	rt.*,
	a.address_dwid as address_dwid1,
	a2.address_dwid as address_dwid2
from playground.hk_rental_transaction_temp rt
left join masterdata_hk.address a on rt.dw_address_id = a.dw_address_id 
left join masterdata_hk.address a2
	on f_prep_dw_id(rt.city) = f_prep_dw_id(a2.city)
	and f_prep_dw_id(rt.city_area) = f_prep_dw_id(a2.city_area)
	and f_prep_dw_id(rt.city_subarea) = f_prep_dw_id(a2.city_subarea)
	and f_prep_dw_id(rt.corrected_estate_name) = f_prep_dw_id(a2.development)
	and f_prep_dw_id(rt.corrected_phase_name) = f_prep_dw_id(a2.development_phase)
	and f_prep_dw_id(rt.corrected_building_name) = f_prep_dw_id(a2.address_building)
	and f_prep_dw_id(rt.corrected_street_num) = f_prep_dw_id(a2.address_number)
	and f_prep_dw_id(rt.corrected_street_name) = f_prep_dw_id(a2.street_name)
)
select *
from base 
where address_dwid1 != address_dwid2 
or (address_dwid1 isnull and address_dwid2 notnull)
or (address_dwid2 isnull and address_dwid1 notnull)
; -- 'address_dwid2' - use masterdata_hk.address to join ids is better 


with base as (
select 
	rt.*,
	p.property_dwid as property_dwid1,
	p2.property_dwid as property_dwid2
from playground.hk_rental_transaction_temp rt
left join masterdata_hk.address a2
	on f_prep_dw_id(rt.city) = f_prep_dw_id(a2.city)
	and f_prep_dw_id(rt.city_area) = f_prep_dw_id(a2.city_area)
	and f_prep_dw_id(rt.city_subarea) = f_prep_dw_id(a2.city_subarea)
	and f_prep_dw_id(rt.corrected_estate_name) = f_prep_dw_id(a2.development)
	and f_prep_dw_id(rt.corrected_phase_name) = f_prep_dw_id(a2.development_phase)
	and f_prep_dw_id(rt.corrected_building_name) = f_prep_dw_id(a2.address_building)
	and f_prep_dw_id(rt.corrected_street_num) = f_prep_dw_id(a2.address_number)
	and f_prep_dw_id(rt.corrected_street_name) = f_prep_dw_id(a2.street_name)
left join masterdata_hk.property_w_old_dw_ids p on rt.dw_property_id = p.dw_property_id 
left join masterdata_hk.property p2 
	on a2.address_dwid = p2.address_dwid 
	and f_prep_dw_id(rt.floor) = f_prep_dw_id(p2.address_floor_text)
	and f_prep_dw_id(rt.flat) = f_prep_dw_id(p2.address_stack)
)
select *
from base 
where property_dwid1 != property_dwid2 
or (property_dwid1 isnull and property_dwid2 notnull)
or (property_dwid2 isnull and property_dwid1 notnull)
; -- 'property_dwid2' - use masterdata_hk.property to join ids is better 



create table playground.hk_rental_transaction_test as
with rtbase as (
select 
	rt.*,
	a2.address_dwid as corrected_address_dwid,
	b.building_dwid ,
	b.project_dwid ,
	p2.property_dwid as corrected_property_dwid
from playground.hk_rental_transaction_temp rt
left join masterdata_hk.address a2
	on f_prep_dw_id(rt.city) = f_prep_dw_id(a2.city)
	and f_prep_dw_id(rt.city_area) = f_prep_dw_id(a2.city_area)
	and f_prep_dw_id(rt.city_subarea) = f_prep_dw_id(a2.city_subarea)
	and f_prep_dw_id(rt.corrected_estate_name) = f_prep_dw_id(a2.development)
	and f_prep_dw_id(rt.corrected_phase_name) = f_prep_dw_id(a2.development_phase)
	and f_prep_dw_id(rt.corrected_building_name) = f_prep_dw_id(a2.address_building)
	and f_prep_dw_id(rt.corrected_street_num) = f_prep_dw_id(a2.address_number)
	and f_prep_dw_id(rt.corrected_street_name) = f_prep_dw_id(a2.street_name)
left join masterdata_hk.building b 
	on a2.address_dwid = b.address_dwid 
left join masterdata_hk.property p2 
	on a2.address_dwid = p2.address_dwid 
	and f_prep_dw_id(rt.floor) = f_prep_dw_id(p2.address_floor_text)
	and f_prep_dw_id(rt.flat) = f_prep_dw_id(p2.address_stack)
)
select 
	--id,
	--activity_dwid,
	corrected_property_dwid as property_dwid,
	corrected_address_dwid as address_dwid,
	a.building_dwid,
	a.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	lower(p.property_type_code || ' ' || p.bedroom_count || '-Rm at ' || coalesce(corrected_estate_name, corrected_building_name)) as activity_name,
	'rental'::text as rent_type,
	property_type_code,
	p.bedroom_count || '-Rm' as property_subtype,
	p.address_unit ,
	p.bedroom_count ,
	p.bathroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	null as land_area_sqm,
	null as gross_floor_area_sqm_min,
	null as gross_floor_area_sqm_max,
	tx_date::date as rent_start_date,
	null as rent_end_date,
	null as rent_amount_weekly,
	price::float as rent_amount_monthly,
	'cn' as country_code,
	data_uuid::uuid as data_uuid,
	null as data_source_uuid,
	'hk-midland-transaction-rent' as data_source,
	dw_property_id ,
	a.slug as original_slug
from rtbase a 
left join masterdata_hk.property p on corrected_property_dwid = p.property_dwid 
; --47847



insert into masterdata_hk.rent_transaction_w_old_dw_ids 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bedroom_count,bathroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
)
select 	
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bedroom_count,bathroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm::float,gross_floor_area_sqm_min::float,gross_floor_area_sqm_max::float,
	rent_start_date,rent_end_date::date,rent_amount_weekly::float,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,dw_property_id,original_slug
from playground.hk_rental_transaction_test;


UPDATE masterdata_hk.rent_transaction_w_old_dw_ids 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; -- 47847


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.rent_transaction_w_old_dw_ids st; -- 0.84258156206240725646


select data_uuid , count(*)
from masterdata_hk.rent_transaction_w_old_dw_ids
group by 1 having count(*) > 1; -- 3087


select activity_dwid  , count(*)
from masterdata_hk.rent_transaction_w_old_dw_ids
group by 1 having count(*) > 1; -- 3087



insert into masterdata_hk.rent_transaction
(
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bedroom_count,bathroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source
)
with base as (
select
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bedroom_count,bathroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,
	row_number() over (partition by data_uuid) as seq
from masterdata_hk.rent_transaction_w_old_dw_ids
)
select 
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bedroom_count,bathroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source
from base
where seq = 1
;


CREATE INDEX rent_transaction_data_uuid_idx ON masterdata_hk.rent_transaction USING btree (data_uuid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_pk PRIMARY KEY (id);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_un_dwid UNIQUE (activity_dwid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT rent_transaction_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
ALTER TABLE masterdata_hk.rent_transaction ADD CONSTRAINT hk_rent_transaction_fk_property_dwid FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);



select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.rent_transaction; -- 0.845196326


select data_uuid , count(*)
from masterdata_hk.rent_transaction
group by 1 having count(*) > 1; -- 0



-- rental_listing


CREATE TABLE masterdata_hk.rent_listing (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	address_local_text text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	CONSTRAINT rent_listing_pk PRIMARY KEY (id),
	CONSTRAINT rent_listing_un UNIQUE (activity_dwid)
);


CREATE TABLE masterdata_hk.rent_listing_w_old_dw_ids (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	address_local_text text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	dw_property_id text NULL,
	original_slug text NULL
);



create table playground.hk_rental_listing_temp as
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill_w_bedroom
    where unit_id notnull and floor notnull
)
--, midland_rental_txn_base as (
    select
        initcap(b.region_name) as city,
        initcap(b.subregion_name) as city_area,
        initcap(b.district_name) as city_subarea,
        b.corrected_estate_name,
        b.corrected_phase_name,
        b.corrected_building_name,
        b.corrected_street_num,
        b.corrected_street_name,
        mu.floor,
        a.*,
        c.dw_address_id,
        c.slug,
        c.fingerprint,
        c.full_address_text,
        c.latitude,
        c.longitude,
        c.postal_code,
        d.dw_property_id,
        row_number() over(partition by serial_no) as rn
    from source.hk_midland_realty_rental_listing a
        left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
        left join midland_units mu on a.unit_id = mu.unit_id and mu.rn = 1
        left join raw_internal.address c
            on f_prep_dw_id(b.region_name) = f_prep_dw_id(c.city)
            and f_prep_dw_id(b.subregion_name) = f_prep_dw_id(c.city_area)
            and f_prep_dw_id(b.district_name) = f_prep_dw_id(c.city_subarea)
            and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
            and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
            and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
            and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
            and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name)
        left join masterdata_hk.z_dm_property d
            on d.dw_address_id = c.dw_address_id
--             and f_prep_dw_id(d.city) = f_prep_dw_id(b.region_name)
--             and f_prep_dw_id(d.city_area) = f_prep_dw_id(b.subregion_name)
--             and f_prep_dw_id(d.city_subarea) = f_prep_dw_id(b.district_name)
--             and f_prep_dw_id(d.project_name)= f_prep_dw_id(b.corrected_estate_name)
--             and f_prep_dw_id(c.development_phase)= f_prep_dw_id(b.corrected_phase_name)
--             and f_prep_dw_id(d.building_name) = f_prep_dw_id(b.corrected_building_name)
            and f_prep_dw_id(d.address_floor) = f_prep_dw_id(mu.floor)
            and f_prep_dw_id(d.address_stack) = f_prep_dw_id(a.flat)
; -- 68337


-- first_pub_date, post_date, update_date difference; could 'update_date' be 'transaction_date'? first_pub_date, post_date which one is first_listing_date / last_listing_date?

create table playground.hk_rental_listing_test as
with rtbase as (
select 
	rt.*,
	a2.address_dwid as corrected_address_dwid,
	b.building_dwid ,
	b.project_dwid ,
	p2.property_dwid as corrected_property_dwid
from playground.hk_rental_listing_temp rt
left join masterdata_hk.address a2
	on f_prep_dw_id(rt.city) = f_prep_dw_id(a2.city)
	and f_prep_dw_id(rt.city_area) = f_prep_dw_id(a2.city_area)
	and f_prep_dw_id(rt.city_subarea) = f_prep_dw_id(a2.city_subarea)
	and f_prep_dw_id(rt.corrected_estate_name) = f_prep_dw_id(a2.development)
	and f_prep_dw_id(rt.corrected_phase_name) = f_prep_dw_id(a2.development_phase)
	and f_prep_dw_id(rt.corrected_building_name) = f_prep_dw_id(a2.address_building)
	and f_prep_dw_id(rt.corrected_street_num) = f_prep_dw_id(a2.address_number)
	and f_prep_dw_id(rt.corrected_street_name) = f_prep_dw_id(a2.street_name)
left join masterdata_hk.building b 
	on a2.address_dwid = b.address_dwid 
left join masterdata_hk.property p2 
	on a2.address_dwid = p2.address_dwid 
	and f_prep_dw_id(rt.floor) = f_prep_dw_id(p2.address_floor_text)
	and f_prep_dw_id(rt.flat) = f_prep_dw_id(p2.address_stack)
)
--, hk_rental_listing_test as (
select 
	--id,
	--activity_dwid,
	corrected_property_dwid as property_dwid,
	corrected_address_dwid as address_dwid,
	a.building_dwid,
	a.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	lower(coalesce(p.bedroom_count || ' bdr ', '') || p.property_type_code || ' at ' || coalesce(corrected_estate_name, corrected_building_name)) as activity_display_text,
	null as listing_status,
	'rent'::text as rent_type,
	property_type_code,
	p.bedroom_count || ' bdr' as property_subtype,
	p.address_unit ,
	a2.full_address_text as address_local_text,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	null::float as land_area_sqm,
	null::float as gross_floor_area_sqm_min,
	null::float as gross_floor_area_sqm_max,
	post_date::date as first_listing_date,
	null::date as last_listing_date,
	null::date as transaction_date,
	null::float as rent_amount_weekly,
	rent::float as rent_amount_monthly,
	'cn' as country_code,
	data_uuid::uuid as data_uuid,
	null as data_source_uuid,
	'hk-midland-listing-rent' as data_source,
	null as status_code,
	null as listing_agent_dwid,
	null as address_lot_number,
	dw_property_id ,
	a.slug as original_slug
from rtbase a 
left join masterdata_hk.property p on corrected_property_dwid = p.property_dwid 
left join masterdata_hk.address a2 on corrected_address_dwid = a2.address_dwid 
;-- 68490


insert into masterdata_hk.rent_listing_w_old_dw_ids 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,
	address_local_text,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,dw_property_id,original_slug
)
select 	
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,
	address_local_text,bathroom_count,bedroom_count,gross_floor_area_sqm::float,net_floor_area_sqm::float,land_area_sqm::float,gross_floor_area_sqm_min::float,gross_floor_area_sqm_max::float,
	first_listing_date::date,last_listing_date::date,transaction_date::date,rent_amount_weekly::float,rent_amount_monthly::float,
	country_code,data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,dw_property_id,original_slug
from playground.hk_rental_listing_test;


UPDATE masterdata_hk.rent_listing_w_old_dw_ids 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; -- 68,490


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.rent_listing_w_old_dw_ids st; -- 0.79005694261936049058


select data_uuid , count(*)
from masterdata_hk.rent_listing_w_old_dw_ids
group by 1 having count(*) > 1; -- 3,799


insert into masterdata_hk.rent_listing
(
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
)
with base as (
select
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,
	row_number() over (partition by data_uuid) as seq
from masterdata_hk.rent_listing_w_old_dw_ids
)
select 
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	country_code,data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
from base
where seq = 1
; -- 64671


CREATE INDEX rent_listing_data_uuid_idx ON masterdata_hk.rent_listing USING btree (data_uuid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_pk PRIMARY KEY (id);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_un_dwid UNIQUE (activity_dwid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT rent_listing_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
ALTER TABLE masterdata_hk.rent_listing ADD CONSTRAINT hk_rent_listing_fk_property_dwid FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.rent_listing; -- 0.7897


select data_uuid , count(*)
from masterdata_hk.rent_listing
group by 1 having count(*) > 1; -- 0




-- sale_listing


CREATE TABLE masterdata_hk.sale_listing (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	unit_count int4 NULL,
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
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	listing_amount numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	CONSTRAINT sale_listing_pk PRIMARY KEY (id),
	CONSTRAINT sale_listing_un UNIQUE (activity_dwid)
);


CREATE TABLE masterdata_hk.sale_listing_w_old_dw_ids (
	id int8 NOT NULL DEFAULT nextval('metadata.activity_id_seq'::regclass),
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	unit_count int4 NULL,
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
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	listing_amount numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	dw_property_id text NULL,
	original_slug text NULL
);


-- TO DO:
create table playground.hk_sale_listing_temp as
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill_w_bedroom
    where unit_id notnull and floor notnull
)
--, midland_rental_txn_base as (
    select
        initcap(b.region_name) as city,
        initcap(b.subregion_name) as city_area,
        initcap(b.district_name) as city_subarea,
        b.corrected_estate_name,
        b.corrected_phase_name,
        b.corrected_building_name,
        b.corrected_street_num,
        b.corrected_street_name,
        mu.floor,
        a.*,
        c.dw_address_id,
        c.slug,
        c.fingerprint,
        c.full_address_text,
        c.latitude,
        c.longitude,
        c.postal_code,
        d.dw_property_id,
        row_number() over(partition by serial_no) as rn
    from source.hk_midland_realty_sale_listing a
        left join reference.hk_midland_hkpost_correction_backfill b using (building_id)
        left join midland_units mu on a.unit_id = mu.unit_id and mu.rn = 1
        left join raw_internal.address c
            on f_prep_dw_id(b.region_name) = f_prep_dw_id(c.city)
            and f_prep_dw_id(b.subregion_name) = f_prep_dw_id(c.city_area)
            and f_prep_dw_id(b.district_name) = f_prep_dw_id(c.city_subarea)
            and f_prep_dw_id(b.corrected_estate_name)= f_prep_dw_id(c.development)
            and f_prep_dw_id(b.corrected_phase_name)= f_prep_dw_id(c.development_phase)
            and f_prep_dw_id(b.corrected_building_name)= f_prep_dw_id(c.address_building)
            and f_prep_dw_id(b.corrected_street_num)= f_prep_dw_id(c.address_number)
            and f_prep_dw_id(b.corrected_street_name)= f_prep_dw_id(c.street_name)
        left join masterdata_hk.z_dm_property d
            on d.dw_address_id = c.dw_address_id
--             and f_prep_dw_id(d.city) = f_prep_dw_id(b.region_name)
--             and f_prep_dw_id(d.city_area) = f_prep_dw_id(b.subregion_name)
--             and f_prep_dw_id(d.city_subarea) = f_prep_dw_id(b.district_name)
--             and f_prep_dw_id(d.project_name)= f_prep_dw_id(b.corrected_estate_name)
--             and f_prep_dw_id(c.development_phase)= f_prep_dw_id(b.corrected_phase_name)
--             and f_prep_dw_id(d.building_name) = f_prep_dw_id(b.corrected_building_name)
            and f_prep_dw_id(d.address_floor) = f_prep_dw_id(mu.floor)
            and f_prep_dw_id(d.address_stack) = f_prep_dw_id(a.flat)
; -- 100384



create table playground.hk_sale_listing_test as
with rtbase as (
select 
	rt.*,
	a2.address_dwid as corrected_address_dwid,
	b.building_dwid ,
	b.project_dwid ,
	p2.property_dwid as corrected_property_dwid
from playground.hk_sale_listing_temp rt
left join masterdata_hk.address a2
	on f_prep_dw_id(rt.city) = f_prep_dw_id(a2.city)
	and f_prep_dw_id(rt.city_area) = f_prep_dw_id(a2.city_area)
	and f_prep_dw_id(rt.city_subarea) = f_prep_dw_id(a2.city_subarea)
	and f_prep_dw_id(rt.corrected_estate_name) = f_prep_dw_id(a2.development)
	and f_prep_dw_id(rt.corrected_phase_name) = f_prep_dw_id(a2.development_phase)
	and f_prep_dw_id(rt.corrected_building_name) = f_prep_dw_id(a2.address_building)
	and f_prep_dw_id(rt.corrected_street_num) = f_prep_dw_id(a2.address_number)
	and f_prep_dw_id(rt.corrected_street_name) = f_prep_dw_id(a2.street_name)
left join masterdata_hk.building b 
	on a2.address_dwid = b.address_dwid 
left join masterdata_hk.property p2 
	on a2.address_dwid = p2.address_dwid 
	and f_prep_dw_id(rt.floor) = f_prep_dw_id(p2.address_floor_text)
	and f_prep_dw_id(rt.flat) = f_prep_dw_id(p2.address_stack)
)
select 
	--id,
	--activity_dwid,
	corrected_property_dwid as property_dwid,
	corrected_address_dwid as address_dwid,
	a.building_dwid,
	a.project_dwid,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	lower(coalesce(p.bedroom_count || ' bdr ', '') || p.property_type_code || ' at ' || coalesce(corrected_estate_name, corrected_building_name)) as activity_display_text,
	null as listing_status,
	null::int as unit_count,
	'sale'::text as sale_type,
	null as sale_subtype,	
	bc.completion_year as property_completion_year,
	property_type_code,
	p.address_unit ,
	a2.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	null::float as land_area_sqm,
	post_date::date as first_listing_date,
	null::date as last_listing_date,
	null::date as transaction_date,
	case when a.hos is True then coalesce(nullif(a.price::float, 0), a.price_hos::float) 
		else a.price::float end as listing_amount,
	'cn' as country_code,
	data_uuid::uuid as data_uuid,
	null as data_source_uuid,
	'hk-midland-listing-sale' as data_source,
	null as status_code,
	null as listing_agent_dwid,
	null as address_lot_number,
	dw_property_id ,
	a.slug as original_slug
from rtbase a 
left join masterdata_hk.property p on corrected_property_dwid = p.property_dwid 
left join masterdata_hk.address a2 on corrected_address_dwid = a2.address_dwid 
left join feature_hk.de__building__completion_year_info bc on a.building_dwid = bc.building_dwid 
left join masterdata_hk.project pj on a.project_dwid = pj.project_dwid 
; -- 100386


insert into masterdata_hk.sale_listing_w_old_dw_ids 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,dw_property_id,original_slug
)
select 	
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year::int,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date::date,last_listing_date::date,transaction_date::date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,dw_property_id,original_slug
from playground.hk_sale_listing_test;


UPDATE masterdata_hk.sale_listing_w_old_dw_ids 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; -- 100386


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.sale_listing_w_old_dw_ids st; -- 0.80187476341322495169


select data_uuid , count(*)
from masterdata_hk.sale_listing_w_old_dw_ids
group by 1 having count(*) > 1; -- 5,788


insert into masterdata_hk.sale_listing
(
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
)
with base as (
select
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,
	row_number() over (partition by data_uuid) as seq
from masterdata_hk.sale_listing_w_old_dw_ids
)
select 
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
from base
where seq = 1
; -- 94558


CREATE INDEX sale_listing_data_uuid_idx ON masterdata_hk.sale_listing USING btree (data_uuid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_pk PRIMARY KEY (id);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_un_dwid UNIQUE (activity_dwid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT sale_listing_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);
ALTER TABLE masterdata_hk.sale_listing ADD CONSTRAINT hk_sale_listing_fk_property_dwid FOREIGN KEY (property_type_code) REFERENCES masterdata_type.property_type(property_type_code);


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.sale_listing; -- 0.8013283


select data_uuid , count(*)
from masterdata_hk.sale_listing
group by 1 having count(*) > 1; -- 0


-- sale_listing new launch
'''
with base as (
select * --data_uuid
from "source".hk_midland_realty_sale_listing
where prev_price isnull and prev_price_hos isnull and extract ('year' from building_first_op_date) >= extract ('year' from update_date)
)
update masterdata_hk.sale_listing a
set sale_subtype = 'resale'
where not exists (select 1 from base b where a.data_uuid = b.data_uuid); -- 
'''

-- before nsert new launch sl records
update masterdata_hk.sale_listing a
set sale_subtype = 'resale'; 



-- TO DO:
-- identify listings for new launch projects versus completed projects.  https://gitlab.com/reapl1/data/pipeline-dags/-/issues/1587
--> sale_subtype = 'resale' / 'new-sale'


with sl_base as (
select b.property_dwid , a.*
from masterdata_hk.z_dm_property_activity_new_launch a
left join masterdata_hk.property_w_old_dw_ids b on a.dw_property_id = b.dw_property_id 
where a.activity_type = 'sale_listing'
)
select property_dwid notnull, count(*)
from sl_base
group by 1;
-- false	188; true	3457

--create table playground.hk_sale_listing_new_launch_test as


select 
	--id,
	--activity_dwid,
	b.property_dwid,
	p.address_dwid , 
	p.building_dwid ,
	p.project_dwid ,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	lower(coalesce(p.bedroom_count || ' bdr ', '') || p.property_type_code || ' at ' || coalesce(pj.project_name_text, bd.building_name_text)) as activity_display_text,
	null as listing_status,
	null::int as unit_count,
	'sale'::text as sale_type,
	'new-sale' as sale_subtype,
	bc.completion_year as property_completion_year,
	p.property_type_code ,
	p.address_unit ,
	ad.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	null::float as land_area_sqm,
	a.activity_date::date as first_listing_date,
	null::date as last_listing_date,
	null::date as transaction_date,
	a.activity_amount as listing_amount,
	'cn' as country_code,
	null::uuid as data_uuid,
	-- sch.id::text as data_uuid, 
	-- a.activity_source_id::text::uuid as data_uuid,
	null as data_source_uuid,
	'rea-manual' as data_source,
	null as status_code,
	null as listing_agent_dwid,
	null as address_lot_number,
	a.dw_property_id ,
	a.slug as original_slug
from masterdata_hk.z_dm_property_activity_new_launch_v2 a
left join masterdata_hk.property_w_old_dw_ids b on a.dw_property_id = b.dw_property_id 
left join masterdata_hk.property p on b.property_dwid = p.property_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join masterdata_hk.building bd on p.building_dwid = bd.building_dwid 
left join feature_hk.de__building__completion_year_info bc on p.building_dwid = bc.building_dwid 
left join masterdata_hk.address ad on p.address_dwid = ad.address_dwid 
--left join reference.hk_new_launch_schematic sch 
	--on sch.num_of_bedrooms = p.bedroom_count and sch.num_of_bathrooms = p.bathroom_count and sch.developer_price = a.activity_amount and sch.floor = p.address_floor_text and sch.stack = p.address_stack 
where a.activity_type = 'sale_listing';

)
select count(*) from base; --2945

SELECT x.* FROM reference.hk_new_launch_schematic x
WHERE developer_price notnull and num_of_bedrooms = 1 and num_of_bathrooms = 1 and developer_price = 10520000 and floor = '29' and stack = 'E';

select data_uuid, count(*) 
from playground.hk_sale_listing_new_launch_test 
group by data_uuid having count(*) > 1; -- 	2923 (use hk_new_launch_schematic.id as data_uuid)




insert into masterdata_hk.sale_listing_w_old_dw_ids 
(
	property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number,dw_property_id,original_slug
)
select 
	--id,
	--activity_dwid,
	b.property_dwid,
	p.address_dwid , 
	p.building_dwid ,
	p.project_dwid ,
	null as lot_group_dwid,
	null as current_lot_group_dwid,
	lower(coalesce(p.bedroom_count || ' bdr ', '') || p.property_type_code || ' at ' || coalesce(pj.project_name_text, bd.building_name_text)) as activity_display_text,
	null as listing_status,
	null::int as unit_count,
	'sale'::text as sale_type,
	'new-sale' as sale_subtype,
	bc.completion_year::int as property_completion_year,
	p.property_type_code ,
	p.address_unit ,
	ad.full_address_text as address_local_text,
	pj.tenure_code as tenure_code,
	p.bathroom_count ,
	p.bedroom_count ,
	p.gross_floor_area_sqm ,
	p.net_floor_area_sqm ,
	null::float as land_area_sqm,
	a.activity_date::date as first_listing_date,
	null::date as last_listing_date,
	null::date as transaction_date,
	a.activity_amount as listing_amount,
	'cn' as country_code,
	null as data_uuid,
	null as data_source_uuid,
	'rea-manual' as data_source,
	null as status_code,
	null as listing_agent_dwid,
	null as address_lot_number,
	a.dw_property_id ,
	a.slug as original_slug
from masterdata_hk.z_dm_property_activity_new_launch_v2 a
left join masterdata_hk.property_w_old_dw_ids b on a.dw_property_id = b.dw_property_id 
left join masterdata_hk.property p on b.property_dwid = p.property_dwid 
left join masterdata_hk.project pj on p.project_dwid = pj.project_dwid 
left join masterdata_hk.building bd on p.building_dwid = bd.building_dwid 
left join feature_hk.de__building__completion_year_info bc on p.building_dwid = bc.building_dwid 
left join masterdata_hk.address ad on p.address_dwid = ad.address_dwid 
where a.activity_type = 'sale_listing';


UPDATE masterdata_hk.sale_listing_w_old_dw_ids 
SET activity_dwid = md5(country_code||'__'||'activity'||'__'||id) 
WHERE activity_dwid isnull ; -- 2945



insert into masterdata_hk.sale_listing
(
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
)
select
	id,activity_dwid,property_dwid,address_dwid,building_dwid,project_dwid,lot_group_dwid,current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,address_unit,
	address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,country_code,
	data_uuid,data_source_uuid,data_source,status_code,listing_agent_dwid,address_lot_number
from masterdata_hk.sale_listing_w_old_dw_ids
where data_uuid isnull; --2945


select sum(case when property_dwid notnull then 1 else 0 end)*1.0/count(*)
from masterdata_hk.sale_listing_w_old_dw_ids st; -- 0.80187476341322495169 --> 0.79934385615159051979


-- move pre map tables

select f_clone_table('map_hk', 'midland_unit_to_dwid_backup2', 'premap_hk', 'midland_unit_to_dwid_backup2', TRUE, TRUE);

select f_clone_table('map_hk', 'midland_unit_to_dwid', 'premap_hk', 'midland_unit_to_dwid', TRUE, TRUE);

select f_clone_table('map_hk', 'midland_building_to_dwid_backup2', 'premap_hk', 'midland_building_to_dwid_backup2', TRUE, TRUE);

select f_clone_table('map_hk', 'midland_building_to_dwid', 'premap_hk', 'midland_building_to_dwid', TRUE, TRUE);

select f_clone_table('map_hk', 'hk_midland_hkpost_correction_backfill', 'premap_hk', 'hk_midland_hkpost_correction_backfill', TRUE, TRUE);

select f_clone_table('map_hk', 'hk_centanet_hkpost_correction_backfill_backup2', 'premap_hk', 'hk_centanet_hkpost_correction_backfill_backup2', TRUE, TRUE);

select f_clone_table('map_hk', 'hk_centanet_hkpost_correction_backfill_backup', 'premap_hk', 'hk_centanet_hkpost_correction_backfill_backup', TRUE, TRUE);

select f_clone_table('map_hk', 'hk_centanet_hkpost_correction_backfill', 'premap_hk', 'hk_centanet_hkpost_correction_backfill', TRUE, TRUE);

select f_clone_table('map_hk', 'hk_centanet_clean_unit', 'premap_hk', 'hk_centanet_clean_unit', TRUE, TRUE);


ALTER TABLE premap_hk.midland_unit_to_dwid ADD CONSTRAINT midland_unit_to_dwid_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE premap_hk.midland_unit_to_dwid ADD CONSTRAINT midland_unit_to_dwid_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE premap_hk.midland_unit_to_dwid ADD CONSTRAINT midland_unit_to_dwid_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE premap_hk.midland_unit_to_dwid ADD CONSTRAINT midland_unit_to_dwid_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);


ALTER TABLE premap_hk.midland_building_to_dwid ADD CONSTRAINT midland_building_to_dwid_fk_address_dwid FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE premap_hk.midland_building_to_dwid ADD CONSTRAINT midland_building_to_dwid_fk_building_dwid FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE premap_hk.midland_building_to_dwid ADD CONSTRAINT midland_building_to_dwid_fk_project_dwid FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);


ALTER TABLE premap_hk.hk_centanet_hkpost_correction_backfill ADD CONSTRAINT centanet_to_address_fk FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE premap_hk.hk_centanet_hkpost_correction_backfill ADD CONSTRAINT centanet_to_building_fk FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE premap_hk.hk_centanet_hkpost_correction_backfill ADD CONSTRAINT centanet_to_project_fk FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);


ALTER TABLE premap_hk.hk_centanet_clean_unit ADD CONSTRAINT centanet_unit_to_address_fk FOREIGN KEY (address_dwid) REFERENCES masterdata_hk.address(address_dwid);
ALTER TABLE premap_hk.hk_centanet_clean_unit ADD CONSTRAINT centanet_unit_to_building_fk FOREIGN KEY (building_dwid) REFERENCES masterdata_hk.building(building_dwid);
ALTER TABLE premap_hk.hk_centanet_clean_unit ADD CONSTRAINT centanet_unit_to_project_fk FOREIGN KEY (project_dwid) REFERENCES masterdata_hk.project(project_dwid);
ALTER TABLE premap_hk.hk_centanet_clean_unit ADD CONSTRAINT centanet_unit_to_property_fk FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);



-- rental transaction premap and map

CREATE TABLE map_hk.midland_rent_txn__map (
	data_uuid uuid NOT NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	land_parcel_dwid text NULL,
	lot_group_dwid text NULL,
	status_code varchar NULL,
	CONSTRAINT midland_rent_txn__map_pk PRIMARY KEY (data_uuid)
);


insert into map_hk.midland_rent_txn__map
(
	data_uuid,activity_dwid,property_dwid,building_dwid,address_dwid,project_dwid,land_parcel_dwid,lot_group_dwid,status_code
)
select 
	mrt.data_uuid::uuid as data_uuid, 
	rt.activity_dwid ,
	rt.property_dwid , 
	rt.building_dwid ,
	rt.address_dwid ,
	rt.project_dwid ,
	null as land_parcel_dwid,
	null as lot_group_dwid,
	null as status_code
from "source".hk_midland_realty_rental_transaction mrt
left join masterdata_hk.rent_transaction rt on rt.data_uuid = mrt.data_uuid
where mrt.data_uuid notnull
; -- 45002 -- 44747

-- temp wrong action
select a.*
from map_hk.midland_sale_txn__map a 
where exists (select 1 from map_hk.midland_rent_txn__map b where a.data_uuid= b.data_uuid); -- 44747 --> 0
-- fix
delete from map_hk.midland_sale_txn__map a
where exists (select 1 from map_hk.midland_rent_txn__map b where a.data_uuid= b.data_uuid); -- 44747


select rt.*, mrt.*
from "source".hk_midland_realty_rental_transaction mrt
left join masterdata_hk.rent_transaction rt on rt.data_uuid = mrt.data_uuid
left join premap_hk.midland_unit_to_dwid unit on mrt.unit_id = unit.unit_id 
where rt.data_uuid notnull and unit.unit_id notnull
; -- 13,530



-- TODO: 
-- check whether we map wrong dwids using midland_unit_to_dwid
-- check whether need to update midland_sale_txn__map and midland_building_to_dwid, midland_rent_txn__map -- all mapping tables need to have consistent and complete data 
-- update and merge rent_transaction_cr_506 and midland_unit_to_dwid_cr_14


select a.property_dwid notnull, c.property_dwid notnull, a.property_dwid = c.property_dwid , count(*)
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
group by 1,2,3;

'''
false	false	null	6718 -- need further analysis
false	true	null	228  -- done
true	false	null	26782-- done: need to add into midland_unit_to_dwid later -> midland_unit_to_dwid_cr_14
true	true	false	53	 -- done
true	true	true	11053-- no need
-->
false	false	null	6669
true	true	false	3 -- the dwids are indeed correct, the unit_id in source data is incorrect
true	true	true	38776
'''

select a.address_dwid notnull, c.address_dwid notnull, a.address_dwid = c.address_dwid , count(*)
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull
group by 1,2,3;
'''
false	false	null	3564 -- done with c.project_dwid notnull
false	true	null	19   -- done
true	false	null	2152 -- done: need to add into midland_unit_to_dwid later -> midland_unit_to_dwid_cr_14
true	true	false	2	 -- done
true	true	true	981	 -- no need
-->
false	false	null	3553
true	true	true	3180
'''


select 
	c.unit_id, c.property_dwid as property_dwid_c, c.building_dwid as building_dwid_c, c.address_dwid as address_dwid_c, c.project_dwid as project_dwid_c,
	a.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull


--create change request to update dwids in rent_transaction based on property_dwid 
select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-transaction-2022-08-16', 'huying','huying'
); --506

call metadata.sp_add_change_table(506::int, 'hk', replace('rent_transaction', '-', '_'));


insert into branch_hk.rent_transaction_cr_506
select 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid notnull
; -- 228


insert into branch_hk.rent_transaction_cr_506
select 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid != c.property_dwid
; -- 53




with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
, midland_unit_to_dwid as (
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull -- 28510
)
select unit_id, count(*)
from midland_unit_to_dwid
group by 1 having count(*) > 1;



create table branch_hk.midland_unit_to_dwid_cr_14 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull; --24331


select a.*
from branch_hk.midland_unit_to_dwid_cr_14 a
left join premap_hk.midland_unit_to_dwid b on a.unit_id = b.unit_id 
where b.unit_id notnull; -- 220 --> 0


delete from branch_hk.midland_unit_to_dwid_cr_14 a
where exists (select 1 from premap_hk.midland_unit_to_dwid b where a.unit_id = b.unit_id); --182


select a1.*
from branch_hk.midland_unit_to_dwid_cr_14 a1
left join branch_hk.midland_unit_to_dwid_cr_14 a2 on a1.unit_id = a2.unit_id 
where a1.property_dwid != a2.property_dwid or (a1.property_dwid ||a2.property_dwid) isnull;


select a1.*
from premap_hk.midland_unit_to_dwid a1
left join premap_hk.midland_unit_to_dwid a2 on a1.unit_id = a2.unit_id 
where a1.property_dwid != a2.property_dwid or (a1.property_dwid || a2.property_dwid isnull and coalesce(a1.property_dwid, a2.property_dwid) notnull);


update branch_hk.midland_unit_to_dwid_cr_14
set cr_record_action = 'insert'
where unit_id notnull;



insert into branch_hk.midland_unit_to_dwid_cr_14
with base as (
select a1.*, row_number() over(partition by a1.unit_id order by a1.property_dwid isnull) as seq
from premap_hk.midland_unit_to_dwid a1
left join premap_hk.midland_unit_to_dwid a2 on a1.unit_id = a2.unit_id 
where a1.property_dwid != a2.property_dwid or (a1.property_dwid || a2.property_dwid isnull and coalesce(a1.property_dwid, a2.property_dwid) notnull)
)
select
	id,region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	'delete' as cr_record_action
from base where seq = 2; -- 567



insert into branch_hk.rent_transaction_cr_506
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull and a.address_dwid isnull and c.address_dwid isnull and c.project_dwid notnull
and a.id not in (select id from branch_hk.rent_transaction_cr_506)
; --219


insert into branch_hk.rent_transaction_cr_506
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull and a.address_dwid isnull and c.address_dwid notnull
and a.id not in (select id from branch_hk.rent_transaction_cr_506)
; -- 18



insert into branch_hk.rent_transaction_cr_506
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull and a.address_dwid != c.address_dwid
and a.id not in (select id from branch_hk.rent_transaction_cr_506)
; -- 2



insert into branch_hk.midland_unit_to_dwid_cr_14 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull and c.address_dwid isnull and a.address_dwid notnull
; -- 2012

CREATE TABLE change_log_hk.rent_transaction_change_log (
	id int8 NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_name text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	rent_start_date date NULL,
	rent_end_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	cr_record_action varchar NULL,
	cr_action_date date NULL,
	cr_id int4 NULL,
	cr_change_id int4 NULL
);

CREATE TABLE change_log_hk.rent_listing_change_log (
	id int8 NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	rent_type text NULL,
	property_type_code text NULL,
	property_subtype text NULL,
	address_unit text NULL,
	address_local_text text NULL,
	bathroom_count int4 NULL,
	bedroom_count int4 NULL,
	gross_floor_area_sqm numeric NULL,
	net_floor_area_sqm numeric NULL,
	land_area_sqm numeric NULL,
	gross_floor_area_sqm_min numeric NULL,
	gross_floor_area_sqm_max numeric NULL,
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	rent_amount_weekly numeric NULL,
	rent_amount_monthly numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	cr_record_action varchar NOT NULL,
	cr_action_date date NULL,
	cr_id int4 NULL,
	cr_change_id int4 NOT NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	CONSTRAINT rent_listing_change_log_pk PRIMARY KEY (cr_change_id)
);

CREATE TABLE change_log_hk.sale_listing_change_log (
	id int8 NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	address_dwid text NULL,
	building_dwid text NULL,
	project_dwid text NULL,
	lot_group_dwid text NULL,
	current_lot_group_dwid text NULL,
	activity_display_text text NULL,
	listing_status text NULL,
	unit_count int4 NULL,
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
	first_listing_date date NULL,
	last_listing_date date NULL,
	transaction_date date NULL,
	listing_amount numeric NULL,
	country_code text NULL,
	data_uuid uuid NULL,
	data_source_uuid text NULL,
	data_source text NULL,
	cr_record_action varchar NOT NULL,
	cr_action_date date NULL,
	cr_id int4 NULL,
	cr_change_id int4 NOT NULL,
	status_code varchar NULL,
	listing_agent_dwid varchar NULL,
	address_lot_number varchar NULL,
	CONSTRAINT sale_listing_change_log_pk PRIMARY KEY (cr_change_id)
);


call metadata.sp_submit_change_request(506, 'huying');

call metadata.sp_approve_change_request(506, 'huying');

call metadata.sp_merge_change_request(506);


update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_14 b
where a.id = b.id::int and b.cr_record_action = 'update'; --0


delete from premap_hk.midland_unit_to_dwid a
where exists (select 1 from branch_hk.midland_unit_to_dwid_cr_14 b where a.id = b.id::int and b.cr_record_action = 'delete'); --567


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_14
where cr_record_action = 'insert'
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 26132




select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-transaction-2022-08-17', 'huying','huying'
); --512

call metadata.sp_add_change_table(512::int, 'hk', replace('rent_transaction', '-', '_'));



insert into branch_hk.rent_transaction_cr_512
select 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid notnull
; -- 61

insert into branch_hk.rent_transaction_cr_512
select 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid != c.property_dwid
; -- 15


create table branch_hk.midland_unit_to_dwid_cr_15 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull; --242




call metadata.sp_submit_change_request(512, 'huying');

call metadata.sp_approve_change_request(512, 'huying');

call metadata.sp_merge_change_request(512);


with midland_unit_to_dwid_cr as (
select unit_id, property_dwid, building_dwid, address_dwid, project_dwid, cr_record_action
from branch_hk.midland_unit_to_dwid_cr_15
group by 1,2,3,4,5,6 --191
)
update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from midland_unit_to_dwid_cr b
where a.unit_id = b.unit_id and b.cr_record_action = 'update'; --242





select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-transaction-2022-08-17', 'huying','huying'
); --513

call metadata.sp_add_change_table(513::int, 'hk', replace('rent_transaction', '-', '_'));


insert into branch_hk.rent_transaction_cr_513
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid notnull
; -- 10


insert into branch_hk.rent_transaction_cr_513
select 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid != c.property_dwid
; -- 6


call metadata.sp_submit_change_request(513, 'huying');

call metadata.sp_approve_change_request(513, 'huying');

call metadata.sp_merge_change_request(513);



----- midland_rent_txn__map

select a.*
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; --595 --> 0

(
select activity_dwid from branch_hk.rent_transaction_cr_506
union
select activity_dwid from branch_hk.rent_transaction_cr_512
union
select activity_dwid from branch_hk.rent_transaction_cr_513
-- 602
)
except
select a.activity_dwid 
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
;
'''
529a5aad2fd95fa2b5b16163f323dc60
f4552d3765fbd1b2e08ccc3b2d8864f0
24fcc3ea131528d8fd2d5c0f004d0199
8f2996ea30e93d4cfd300375933a467c
168e442df2d804b5f973c05f601bb51e
3891d340b2d4486933a8e7d3116857ce
7e0aef9445b36cbae5378f6f53029b62
'''

create table branch_hk.midland_rent_txn__map_cr_1 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 595



update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_txn__map_cr_1 b
where a.activity_dwid = b.activity_dwid; -- 595



	
----- midland_building_to_dwid

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
;-- update 331 --> 324 --> 0

select building_id , building_dwid, address_dwid, project_dwid, 
	ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
from premap_hk.midland_unit_to_dwid a
where not exists (select 1 from premap_hk.midland_building_to_dwid b where a.building_id = b.building_id) -- 70
group by 1,2,3,4;-- insert 70? --> only 'B000061339' is left 


create table branch_hk.midland_unit_to_dwid_cr_16 as 
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
select 
	c.id,c.region_name,c.region_id,c.district_name,c.district_id,c.subregion_name,c.subregion_id,
	c.estate_name,c.estate_id,c.phase_name,c.phase_id,
	c.building_name,c.building_id,c.address_number,c.address_street,c.floor,c.stack,c.unit_id,
	c.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,c.lot_group_dwid,
	'update' as cr_record_action
--a.*, c.*
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
left join premap_hk.midland_unit_to_dwid c on b.building_id = c.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and b.building_dwid isnull and b.address_dwid isnull and b.project_dwid isnull
; --414


update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_16 b
where a.id = b.id and b.cr_record_action = 'update'; --414


select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-transaction-2022-08-17', 'huying','huying'
); --514
call metadata.sp_add_change_table(514::int, 'hk', replace('rent_transaction', '-', '_'));

insert into branch_hk.rent_transaction_cr_514
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull and a.address_dwid isnull and c.address_dwid notnull
; -- 64

call metadata.sp_submit_change_request(514, 'huying');

call metadata.sp_approve_change_request(514, 'huying');

call metadata.sp_merge_change_request(514);



create table branch_hk.midland_building_to_dwid_cr_10 as 
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
select 
	id,region_name,region_id,district_name,district_id,subregion_name,subregion_id,
	estate_name,estate_id,phase_name,phase_id,building_name,a.building_id,address_number,address_street,
	b.building_dwid,b.address_dwid,b.project_dwid,lot_group_dwid,
	'update' as cr_record_action
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and a.building_id notnull;


update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_building_to_dwid_cr_10 b
where a.id = b.id and b.cr_record_action = 'update'; -- 324


insert into branch_hk.midland_building_to_dwid_cr_10
with idbase as (
select building_id , building_dwid, address_dwid, project_dwid, 
	ROW_NUMBER() over (PARTITION BY building_id order by building_dwid, address_dwid, project_dwid) AS seq
from premap_hk.midland_unit_to_dwid a
where not exists (select 1 from premap_hk.midland_building_to_dwid b where a.building_id = b.building_id) -- 70
group by 1,2,3,4
)
, cr_base as (
select 
	null as id,c.region_name,c.region_id,c.district_name,c.district_id,c.subregion_name,c.subregion_id,
	c.estate_name,c.estate_id,c.phase_name,c.phase_id,c.building_name,b.building_id,c.address_number,c.address_street,
	b.building_dwid,b.address_dwid,b.project_dwid,c.lot_group_dwid,'insert' as cr_record_action,
	row_number() over (partition by c.region_name, c.district_name, c.subregion_name, c.estate_name, c.phase_name, c.building_name, b.building_id) as seq
from idbase b
left join premap_hk.midland_building_to_dwid a on a.building_id = b.building_id
left join premap_hk.midland_unit_to_dwid c on b.building_id = c.building_id 
where b.seq = 1
--where a.building_id isnull --and b.address_dwid notnull
)
select 
	id::int,region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,
	phase_name,phase_id,building_name,building_id,address_number,address_street,
	building_dwid,address_dwid,project_dwid,lot_group_dwid,cr_record_action
from cr_base where seq = 1; --71


insert into premap_hk.midland_building_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,
	phase_name,phase_id,building_name,building_id,address_number,address_street,
	building_dwid,address_dwid,project_dwid,lot_group_dwid
)
select distinct 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,
	phase_name,phase_id,building_name,building_id,address_number,address_street,
	building_dwid,address_dwid,project_dwid,lot_group_dwid
from branch_hk.midland_building_to_dwid_cr_10
where cr_record_action = 'insert'; -- 71





---------------- TO DO:
select a.*, c.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
;


create table branch_hk.midland_unit_to_dwid_cr_17 as 
with update_base as (
select a.activity_dwid , a.property_dwid , a.address_dwid , a.building_dwid , a.project_dwid , a.data_uuid ,
	c.building_id , c.unit_id 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
)
select unit_id, property_dwid, address_dwid, building_dwid, project_dwid, 'update' as cr_record_action
from update_base
group by 1,2,3,4,5 order by 1; --409

update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_17 b
where a.unit_id = b.unit_id and b.cr_record_action = 'update'; --409


create table branch_hk.midland_building_to_dwid_cr_11 as 
with update_base as (
select a.activity_dwid , a.property_dwid , a.address_dwid , a.building_dwid , a.project_dwid , a.data_uuid ,
	c.building_id , c.unit_id 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
)
select building_id, address_dwid, building_dwid, project_dwid, 'update' as cr_record_action
from update_base
group by 1,2,3,4 order by 1; --3


create table branch_hk.midland_building_to_dwid_cr_12 as 
with update_base as (
select a.activity_dwid , a.property_dwid , a.address_dwid , a.building_dwid , a.project_dwid , a.data_uuid ,
	c.building_id , c.unit_id 
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where c.unit_id in (select unit_id from branch_hk.midland_unit_to_dwid_cr_17)
)
select building_id, address_dwid, building_dwid, project_dwid, 'update' as cr_record_action
from update_base
group by 1,2,3,4 order by 1; --174

update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_building_to_dwid_cr_11 b
where a.building_id = b.building_id and b.cr_record_action = 'update'; -- 3


update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_building_to_dwid_cr_12 b
where a.building_id = b.building_id and b.cr_record_action = 'update'; -- 174


----

select a.*
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
;

create table branch_hk.midland_rent_txn__map_cr_2 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; --64

update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_txn__map_cr_2 b
where a.activity_dwid = b.activity_dwid; -- 64

------

select a.*, c.*
from map_hk.midland_rent_txn__map a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
; -- 3


-----

select a.address_dwid notnull, c.address_dwid notnull, a.address_dwid = c.address_dwid , count(*)
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
group by 1,2,3;

'''
false	false		3026
false	true		453 --> 0
true	true	false	47 --> 0
true	true	true	41225
'''

select c.*, a.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.address_dwid isnull and c.address_dwid notnull
;


select c.*, a.*
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.address_dwid != c.address_dwid
;
-- for unit_id not exists in midland_unit_to_dwid, need to add new records
select mrt.*
from "source".hk_midland_realty_rental_transaction mrt
left join masterdata_hk.rent_transaction rt on rt.data_uuid = mrt.data_uuid
left join premap_hk.midland_unit_to_dwid unit on mrt.unit_id = unit.unit_id 
where rt.data_uuid notnull and unit.unit_id isnull
; -- 2555

select rt.*, mrt.*
from masterdata_hk.rent_transaction rt
left join "source".hk_midland_realty_rental_transaction mrt on rt.data_uuid = mrt.data_uuid
left join premap_hk.midland_unit_to_dwid unit on mrt.unit_id = unit.unit_id 
where rt.data_uuid notnull and unit.unit_id isnull
; --2555

'''
create table branch_hk.midland_unit_to_dwid_cr_18 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.street_num as address_number, d.street_addr as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_realty_building_address d on b.building_id = d.building_id 
--left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.data_uuid notnull and c.unit_id isnull
; -- 


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_18
where cr_record_action = 'insert'
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 26132
'''

select metadata.fn_create_change_request(
    'hk-update-buildingdwid-in-rent-transaction-2022-08-18', 'huying','huying'
); --520
call metadata.sp_add_change_table(522::int, 'hk', replace('rent_transaction', '-', '_'));


insert into branch_hk.rent_transaction_cr_520
select 
	a.id,a.activity_dwid,
	a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid isnull and c.address_dwid notnull --453
;


insert into branch_hk.rent_transaction_cr_521
select 
	a.id,a.activity_dwid,
	a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid != c.address_dwid --47
;


insert into branch_hk.rent_transaction_cr_522
select 
	a.id,a.activity_dwid,
	a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
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
;

call metadata.sp_submit_change_request(520, 'huying');

call metadata.sp_approve_change_request(520, 'huying');

call metadata.sp_merge_change_request(520);


call metadata.sp_submit_change_request(521, 'huying');

call metadata.sp_approve_change_request(521, 'huying');

call metadata.sp_merge_change_request(521);

call metadata.sp_submit_change_request(522, 'huying');

call metadata.sp_approve_change_request(522, 'huying');

call metadata.sp_merge_change_request(522);



create table branch_hk.midland_rent_txn__map_cr_3 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 595
-- merge change request
update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_txn__map_cr_3 b
where a.activity_dwid = b.activity_dwid; -- 595



create table branch_hk.midland_rent_txn__map_cr_4 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 18
-- merge change request
update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_txn__map_cr_4 b
where a.activity_dwid = b.activity_dwid; -- 18




create table branch_hk.midland_unit_to_dwid_cr_18 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_transaction a
left join "source".hk_midland_realty_rental_transaction b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.id notnull and c.property_dwid isnull
--and a.property_dwid notnull and c.property_dwid isnull
; --6295


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_18 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where a.unit_id = b.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 2210



create table branch_hk.midland_unit_to_dwid_cr_19 as 
select c.unit_id , a.property_dwid , a.address_dwid , a.building_dwid , a.project_dwid , 'update' as cr_record_action
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
group by 1,2,3,4,5
; -- 83
		
		

update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_19 b
where a.unit_id = b.unit_id and b.cr_record_action = 'update'; --85



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

ALTER TABLE map_hk.midland_sale_txn__map ADD CONSTRAINT midland_sale_txn__map_fk_activity_dwid FOREIGN KEY (activity_dwid) REFERENCES masterdata_hk.sale_transaction(activity_dwid);
ALTER TABLE map_hk.midland_sale_txn__map ADD CONSTRAINT midland_sale_txn__map_fk_property_dwid FOREIGN KEY (property_dwid) REFERENCES masterdata_hk.property(property_dwid);



select f_clone_table('premap_hk', 'midland_unit_to_dwid', 'premap_hk', 'midland_unit_to_dwid_backup_rt', TRUE, TRUE);
select f_clone_table('premap_hk', 'midland_building_to_dwid', 'premap_hk', 'midland_building_to_dwid_backup_rt', TRUE, TRUE);


-- rental listing premap and map

CREATE TABLE map_hk.midland_rent_listing__map (
	data_uuid uuid NOT NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	land_parcel_dwid text NULL,
	lot_group_dwid text NULL,
	status_code varchar NULL,
	CONSTRAINT midland_rent_listing__map_pk PRIMARY KEY (data_uuid)
);


-- check rent_listing v.s. midland_unit_to_dwid first 

select a.*, c.*
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
-- 28090 --> 1,053 --> 222
	and b.unit_id notnull --> 13 to do! --> 0
; 


select a.property_dwid notnull, c.property_dwid notnull, a.property_dwid = c.property_dwid , count(*)
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where b.unit_id notnull
group by 1,2,3;
'''
false	false	null	13235
false	true	null	436	  -- done: update rent_listing
true	false	null	22964 --> 233 --> 15 -- done: need to add into midland_unit_to_dwid
true	true	false	106	  -- done: update rent_listing / midland_unit_to_dwid
true	true	true	28295
'''

select a.address_dwid notnull, c.address_dwid notnull, a.address_dwid = c.address_dwid , count(*)
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull
and b.unit_id notnull
group by 1,2,3;
'''
false	false	null	6903
false	true	null	287  -- done: update rent_listing
true	false	null	3163 --> 201 --> 1 -- done: need to add into midland_unit_to_dwid
true	true	true	2882
'''

select a.project_dwid notnull, c.project_dwid notnull, a.project_dwid = c.project_dwid , count(*)
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull and a.address_dwid isnull and c.address_dwid isnull
group by 1,2,3;
'''
false	false		6612
false	true		291	 -- done: update rent_listing
'''

select a.building_dwid notnull, c.building_dwid notnull, a.building_dwid = c.building_dwid , count(*)
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull
and b.unit_id notnull
group by 1,2,3;
'''
false	false	null	7078
true	false	null	1028 -- 197 -- done: need to update midland_unit_to_dwid
true	true	true	5136
'''



ALTER TABLE premap_hk.midland_unit_to_dwid ADD CONSTRAINT midland_unit_to_dwid_region_name_district_name_subregion_na_key UNIQUE (region_name, district_name, subregion_name, estate_name, phase_name, building_name, building_id, floor, stack)


create table branch_hk.midland_unit_to_dwid_cr_20 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull
; -- 22667


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_20 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 22413



create table branch_hk.midland_unit_to_dwid_cr_21 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull 
and a.address_dwid notnull and c.address_dwid isnull
; -- 3113


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_21 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 3109



create table branch_hk.midland_unit_to_dwid_cr_23 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull
; -- 236 -- 


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_23 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 13 -- 12



with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_22 a
where cr_record_action = 'insert'
and exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from base1 b
where a.unit_id = b.unit_id and b.seq = 1; --209


select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-listing-2022-08-18', 'huying','huying'
); --523
call metadata.sp_add_change_table(523::int, 'hk', replace('rent_listing', '-', '_'));

insert into branch_hk.rent_listing_cr_523
select distinct  
	a.id,a.activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid notnull
; -- 416

insert into branch_hk.rent_listing_cr_523
select distinct  
	a.id,a.activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid != c.property_dwid
; -- 108


insert into branch_hk.rent_listing_cr_523
select distinct  
	a.id,a.activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid isnull and c.address_dwid notnull
;

call metadata.sp_submit_change_request(523, 'huying');

call metadata.sp_approve_change_request(523, 'huying');

call metadata.sp_merge_change_request(523);





select metadata.fn_create_change_request(
    'hk-update-projectdwid-in-rent-listing-2022-08-18', 'huying','huying'
); --524
call metadata.sp_add_change_table(524::int, 'hk', replace('rent_listing', '-', '_'));



insert into branch_hk.rent_listing_cr_524
select distinct  
	a.id,a.activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid isnull and c.address_dwid isnull
and a.project_dwid isnull and c.project_dwid notnull
;



call metadata.sp_submit_change_request(524, 'huying');

call metadata.sp_approve_change_request(524, 'huying');

call metadata.sp_merge_change_request(524);


drop table branch_hk.midland_unit_to_dwid_cr_24;

create table branch_hk.midland_unit_to_dwid_cr_24 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	c.id as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	case when c.unit_id notnull then 'update'
		else 'insert' end as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull 
and a.building_dwid notnull and c.building_dwid isnull and b.unit_id notnull
; -- 818 update



update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_24 b
where a.id = b.id and b.cr_record_action = 'update'; -- 817



select c.unit_id, a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid  
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
and b.unit_id notnull; -- 13 -- 0

unit_id in ('U000769318', 'U000765591', 'U002413751', 'U000768396', 'U000763802', 'U000763714', 
'U000768044', 'U000761409', 'U000764414', 'U000761449', 'U000761051', 'U000767468', 'U000769313')


-- midland_unit_to_dwid v.s. midland_building_to_dwid

-- need to update midland_building_to_dwid
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
; -- 521 -- 0

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
; -- 526 -- 5

create table branch_hk.midland_building_to_dwid_cr_13 as 
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
select 
	id,region_name,region_id,district_name,district_id,subregion_name,subregion_id,
	estate_name,estate_id,phase_name,phase_id,building_name,a.building_id,address_number,address_street,
	b.building_dwid,b.address_dwid,b.project_dwid,lot_group_dwid,
	'update' as cr_record_action
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and a.building_id notnull;

update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_building_to_dwid_cr_13 b
where a.id = b.id and b.cr_record_action = 'update'; -- 521




-- check rent_listing v.s. midland_building_to_dwid️

select a.*, c.*
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
; -- 455 --> 4


select metadata.fn_create_change_request(
    'hk-update-buildingdwid-in-rent-listing-2022-08-19', 'huying','huying'
); --530
call metadata.sp_add_change_table(530::int, 'hk', replace('rent_listing', '-', '_'));


insert into branch_hk.rent_listing_cr_530
select 
	a.id,a.activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid isnull and c.address_dwid notnull 
; --367

insert into branch_hk.rent_listing_cr_530
select 
	a.id,a.activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid != c.address_dwid 
; -- 71

call metadata.sp_submit_change_request(530, 'huying');

call metadata.sp_approve_change_request(530, 'huying');

call metadata.sp_merge_change_request(530);




select metadata.fn_create_change_request(
    'hk-update-projectdwid-in-rent-listing-2022-08-19', 'huying','huying'
); --531
call metadata.sp_add_change_table(531::int, 'hk', replace('rent_listing', '-', '_'));


insert into branch_hk.rent_listing_cr_531
select 
	a.id,a.activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join "source".hk_midland_realty_rental_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where c.building_id notnull
and md5(
		f_prep_dw_id(a.address_dwid)||'__'||
		f_prep_dw_id(a.building_dwid)||'__'||
		f_prep_dw_id(a.project_dwid)
		) 
	!= md5(
		f_prep_dw_id(c.address_dwid)||'__'||
		f_prep_dw_id(c.building_dwid)||'__'||
		f_prep_dw_id(c.project_dwid)
		) 
;

call metadata.sp_submit_change_request(531, 'huying');

call metadata.sp_approve_change_request(531, 'huying');

call metadata.sp_merge_change_request(531);


-- create midland_rent_listing__map


insert into map_hk.midland_rent_listing__map
(
	data_uuid,activity_dwid,property_dwid,building_dwid,address_dwid,project_dwid,land_parcel_dwid,lot_group_dwid,status_code
)
select 
	mrt.data_uuid::uuid as data_uuid, 
	rt.activity_dwid ,
	rt.property_dwid , 
	rt.building_dwid ,
	rt.address_dwid ,
	rt.project_dwid ,
	null as land_parcel_dwid,
	null as lot_group_dwid,
	null as status_code
from "source".hk_midland_realty_rental_listing mrt
left join masterdata_hk.rent_listing rt on rt.data_uuid = mrt.data_uuid
where mrt.data_uuid notnull
; -- 65504



-- sale listing premap and map

CREATE TABLE map_hk.midland_sale_listing__map (
	data_uuid uuid NOT NULL,
	activity_dwid text NULL,
	property_dwid text NULL,
	building_dwid text NULL,
	address_dwid text NULL,
	project_dwid text NULL,
	land_parcel_dwid text NULL,
	lot_group_dwid text NULL,
	status_code varchar NULL,
	CONSTRAINT midland_sale_listing__map_pk PRIMARY KEY (data_uuid)
);

-- check rent_listing v.s. midland_unit_to_dwid first 

select a.*, c.*
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
-- 43523 --> 4012
and b.unit_id notnull -- 1464
; 

select a.property_dwid notnull, c.property_dwid notnull, a.property_dwid = c.property_dwid , count(*)
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where b.unit_id notnull
group by 1,2,3;
'''
false	false	null	17131
false	true	null	700 	-- done: update sale_listing
true	false	null	34446 -> 384 -> 4	-- to do: need to add into midland_unit_to_dwid
true	true	false	74		-- done: update sale_listing / midland_unit_to_dwid
true	true	true	41448
'''

select a.address_dwid notnull, c.address_dwid notnull, a.address_dwid = c.address_dwid , count(*)
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull
and b.unit_id notnull
group by 1,2,3;
'''
false	false		9330
false	true		278		-- done: update sale_listing
true	false		3752 --> 4	-- done: need to add into midland_unit_to_dwid
true	true	false	4	-- done: update sale_listing
true	true	true	3767
'''

select a.project_dwid notnull, c.project_dwid notnull, a.project_dwid = c.project_dwid , count(*)
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull and a.address_dwid isnull and c.address_dwid isnull
group by 1,2,3;
'''
false	false		10563
false	true		268   -- done: update sale_listing
'''

select a.building_dwid notnull, c.building_dwid notnull, a.building_dwid = c.building_dwid , count(*)
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.property_dwid isnull and c.property_dwid isnull
and b.unit_id notnull
group by 1,2,3;
'''
false	false	null	9539
false	true	null	81	 -- done: update sale_listing
true	false	null	5198 --> 1454 -- done: need to add into midland_unit_to_dwid
true	true	false	2	 -- done: update sale_listing
true	true	true	2311
'''


create table branch_hk.midland_unit_to_dwid_cr_25 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	'insert' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull
; -- 36525


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_25 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 33983



create table branch_hk.midland_unit_to_dwid_cr_26 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	case when c.unit_id notnull then 'update'
	else 'insert' end as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid notnull and c.property_dwid isnull
; -- 2537


update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_26 b
where a.unit_id = b.unit_id and b.cr_record_action = 'update'; -- 370


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_26 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 40


select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-sale-listing-2022-08-19', 'huying','huying'
); --532
call metadata.sp_add_change_table(532::int, 'hk', replace('sale_listing', '-', '_'));

insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid notnull
;

insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid != c.property_dwid
;

insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid isnull and c.address_dwid notnull
;


insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid != c.address_dwid
;


insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid isnull and c.address_dwid isnull
and a.project_dwid isnull and c.project_dwid notnull
;

insert into branch_hk.sale_listing_cr_532
select distinct  
	a.id,activity_dwid,c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid isnull and c.address_dwid isnull
and a.building_dwid != c.building_dwid
;

call metadata.sp_submit_change_request(532, 'huying');

call metadata.sp_approve_change_request(532, 'huying');

call metadata.sp_merge_change_request(532);



create table branch_hk.midland_unit_to_dwid_cr_27 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	case when c.unit_id notnull then 'update'
	else 'insert' end as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull and a.property_dwid isnull and c.property_dwid isnull
and a.address_dwid notnull and c.address_dwid isnull
; -- 4022


insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_27 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 4011



create table branch_hk.midland_unit_to_dwid_cr_28 as 
with midland_units as (
    select unit_id, floor, row_number() over(partition by unit_id order by last_tx_date) as rn
    from reference.hk_midland_backfill
    where unit_id notnull and floor notnull
)
select
	distinct
	null as id,b.region_name,b.region_id,b.district_name,b.district_id,b.subregion_name,b.subregion_id,
	b.estate_name,b.estate_id,b.phase_name,b.phase_id,b.building_name,b.building_id,
	d.corrected_street_num as address_number, d.corrected_street_name as address_street, 
	mu.floor,b.flat as stack, b.unit_id,
	a.property_dwid,a.building_dwid,a.address_dwid,a.project_dwid,a.lot_group_dwid, 
	case when c.unit_id notnull then 'update'
	else 'insert' end as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_unit_to_dwid c on b.unit_id = c.unit_id 
left join reference.hk_midland_hkpost_correction_backfill d on b.building_id = d.building_id 
left join midland_units mu on b.unit_id = mu.unit_id and mu.rn = 1
where a.id notnull 
and md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.property_dwid)||'__'||f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
; -- 3869

insert into premap_hk.midland_unit_to_dwid
(
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
)
with base1 as(
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid,
	row_number() over (partition by region_name, district_name, subregion_name, estate_name, phase_name, building_name, floor, stack order by project_dwid) as seq
from branch_hk.midland_unit_to_dwid_cr_28 a
where cr_record_action = 'insert'
and not exists (select 1 from premap_hk.midland_unit_to_dwid b where b.unit_id = a.unit_id)
)
select 
	region_name,region_id,district_name,district_id,subregion_name,subregion_id,estate_name,estate_id,phase_name,phase_id,
	building_name,building_id,address_number,address_street,floor,stack,unit_id,property_dwid,building_dwid,address_dwid,project_dwid,lot_group_dwid
from base1 where seq = 1; -- 314


update premap_hk.midland_unit_to_dwid a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_unit_to_dwid_cr_28 b
where a.unit_id = b.unit_id and b.cr_record_action = 'update'; -- 1149




-- midland_unit_to_dwid v.s. midland_building_to_dwid

-- need to update midland_building_to_dwid
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
; -- 437 -- 0

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
; -- 445 -- 8


create table branch_hk.midland_building_to_dwid_cr_14 as 
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
select 
	id,region_name,region_id,district_name,district_id,subregion_name,subregion_id,
	estate_name,estate_id,phase_name,phase_id,building_name,a.building_id,address_number,address_street,
	b.building_dwid,b.address_dwid,b.project_dwid,lot_group_dwid,
	'update' as cr_record_action
from premap_hk.midland_building_to_dwid a
left join base b on a.building_id = b.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and a.building_id notnull;


update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_building_to_dwid_cr_14 b
where a.id = b.id and b.cr_record_action = 'update'; -- 437





-- check sale_listing v.s. midland_building_to_dwid️

select a.*, c.*
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid 
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id
where md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
-- 3136 -- 2110	
and c.building_id notnull -- 6 -- 0
; 


select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-sale-listing-2022-08-19', 'huying','huying'
); --534
call metadata.sp_add_change_table(534::int, 'hk', replace('sale_listing', '-', '_'));

insert into branch_hk.sale_listing_cr_534
select distinct  
	a.id,activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid isnull and c.address_dwid notnull 
;-- 877

insert into branch_hk.sale_listing_cr_534
select distinct  
	a.id,activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where a.id notnull and a.address_dwid != c.address_dwid 
;-- 149

call metadata.sp_submit_change_request(534, 'huying');

call metadata.sp_approve_change_request(534, 'huying');

call metadata.sp_merge_change_request(534);



select metadata.fn_create_change_request(
    'hk-update-buildingdwid-in-sale-listing-2022-08-19', 'huying','huying'
); --535
call metadata.sp_add_change_table(535::int, 'hk', replace('sale_listing', '-', '_'));


insert into branch_hk.sale_listing_cr_535
select distinct  
	a.id,activity_dwid,a.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,c.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join "source".hk_midland_realty_sale_listing b on a.data_uuid = b.data_uuid
left join premap_hk.midland_building_to_dwid c on b.building_id = c.building_id 
where c.building_id notnull
and md5(f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(c.address_dwid)||'__'||f_prep_dw_id(c.building_dwid)||'__'||f_prep_dw_id(c.project_dwid)) 
;-- 6


call metadata.sp_submit_change_request(535, 'huying');

call metadata.sp_approve_change_request(535, 'huying');

call metadata.sp_merge_change_request(535);


-- create midland_sale_listing__map

insert into map_hk.midland_sale_listing__map
(
	data_uuid,activity_dwid,property_dwid,building_dwid,address_dwid,project_dwid,land_parcel_dwid,lot_group_dwid,status_code
)
select 
	mrt.data_uuid::uuid as data_uuid, 
	rt.activity_dwid ,
	rt.property_dwid , 
	rt.building_dwid ,
	rt.address_dwid ,
	rt.project_dwid ,
	null as land_parcel_dwid,
	null as lot_group_dwid,
	null as status_code
from "source".hk_midland_realty_sale_listing mrt
left join masterdata_hk.sale_listing rt on rt.data_uuid = mrt.data_uuid
where mrt.data_uuid notnull
;






---------- consistency check again


select metadata.fn_create_change_request(
    'hk-update-propertydwid-in-rent-transaction-2022-08-19', 'huying','huying'
); --536
call metadata.sp_add_change_table(536::int, 'hk', replace('rent_transaction', '-', '_'));


insert into branch_hk.rent_transaction_cr_536
select distinct 
	a.id,a.activity_dwid,
	c.property_dwid,c.address_dwid,c.building_dwid,c.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
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
;


call metadata.sp_submit_change_request(536, 'huying');

call metadata.sp_approve_change_request(536, 'huying');

call metadata.sp_merge_change_request(536);


update masterdata_hk.rent_transaction a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.rent_transaction_cr_536 b
where a.activity_dwid = b.activity_dwid and b.cr_record_action = 'update'; -- 109



update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.rent_transaction_cr_536 b
where a.activity_dwid = b.activity_dwid and b.cr_record_action = 'update'; -- 109



------------------
-- consistency check

select 
(a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int as score_a,
(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int as score_c,
a.*, c.*
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
;

with update_premap_base as (
select distinct c.building_id , a.address_dwid , a.building_dwid , a.project_dwid 
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
and (a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int >=
(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int
-- 12
)
update premap_hk.midland_building_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from update_premap_base b 
where a.building_id = b.building_id
;-- 12


with update_premap_base as (
select distinct c.building_id , a.address_dwid , a.building_dwid , a.project_dwid 
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
and (a.address_dwid notnull)::int+(a.building_dwid notnull)::int+(a.project_dwid notnull)::int >=
(c.address_dwid notnull)::int+(c.building_dwid notnull)::int+(c.project_dwid notnull)::int
)
update premap_hk.midland_unit_to_dwid a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from update_premap_base b 
where a.building_id = b.building_id
;-- 34



---- DONE:
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




-------------------

-- check
select st.*, p.*
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; -- 0 -- done


select st.*, p.*
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; -- 138 -- 72 -- 0 -- done


select st.*, p.*
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid; -- 63


-- update 
select metadata.fn_create_change_request(
    'hk-update-property-2022-08-22', 'huying','huying'
); -- 546

call metadata.sp_add_change_table(546::int, 'hk', replace('property', '-', '_'));


insert into branch_hk.property_cr_546
with correct_idbase as (
select distinct st.property_dwid , st.address_dwid , st.building_dwid , st.project_dwid 
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid notnull
)
select 
	p.id,b.property_dwid,b.address_dwid,b.building_dwid,unit_group_dwid,b.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join correct_idbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull; --12


call metadata.sp_submit_change_request(546, 'huying');

call metadata.sp_approve_change_request(546, 'huying');

call metadata.sp_merge_change_request(546);


select metadata.fn_create_change_request(
    'hk-update-rent-transaction-2022-08-22', 'huying','huying'
); -- 547

call metadata.sp_add_change_table(547::int, 'hk', replace('rent_transaction', '-', '_'));

insert into branch_hk.rent_transaction_cr_547
with correct_idbase as (
select distinct p.property_dwid , p.address_dwid , p.building_dwid , p.project_dwid 
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid isnull
)
select 
	a.id,a.activity_dwid,
	b.property_dwid,b.address_dwid,b.building_dwid,b.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_name,rent_type,property_type_code,property_subtype,address_unit,bathroom_count,bedroom_count,
	gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	rent_start_date,rent_end_date,rent_amount_weekly,rent_amount_monthly,a.country_code,
	a.data_uuid,a.data_source_uuid,a.data_source,'update' as cr_record_action
from masterdata_hk.rent_transaction a
left join correct_idbase b on a.property_dwid = b.property_dwid
where b.property_dwid notnull; --14

call metadata.sp_submit_change_request(547, 'huying');

call metadata.sp_approve_change_request(547, 'huying');

call metadata.sp_merge_change_request(547);


with correct_idbase as (
select distinct p.property_dwid , p.address_dwid , p.building_dwid , p.project_dwid 
from masterdata_hk.rent_transaction st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid isnull
)
update masterdata_hk.rent_transaction a
set building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from correct_idbase b where a.property_dwid = b.property_dwid
and exists (select 1 from correct_idbase b where a.property_dwid = b.property_dwid); -- 14



-- done:
-- check rent_transaction v.s. midland_rent_txn_map
select a.*
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
;
-- change request
create table branch_hk.midland_rent_txn__map_cr_5 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_transaction a
left join map_hk.midland_rent_txn__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 16
-- merge change request
update map_hk.midland_rent_txn__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_txn__map_cr_5 b
where a.activity_dwid = b.activity_dwid; -- 595



-- update 
select metadata.fn_create_change_request(
    'hk-update-property-2022-08-22', 'huying','huying'
); -- 548

call metadata.sp_add_change_table(548::int, 'hk', replace('property', '-', '_'));


insert into branch_hk.property_cr_548
with correct_idbase as (
select distinct st.property_dwid , st.address_dwid , st.building_dwid , st.project_dwid 
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid notnull
)
select 
	p.id,b.property_dwid,b.address_dwid,b.building_dwid,unit_group_dwid,b.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join correct_idbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull; --65


call metadata.sp_submit_change_request(548, 'huying');

call metadata.sp_approve_change_request(548, 'huying');

call metadata.sp_merge_change_request(548);


select metadata.fn_create_change_request(
    'hk-update-sale-listing-2022-08-22', 'huying','huying'
); -- 549

call metadata.sp_add_change_table(549::int, 'hk', replace('sale-listing', '-', '_'));

insert into branch_hk.sale_listing_cr_549
with correct_idbase as (
select distinct p.property_dwid , p.address_dwid , p.building_dwid , p.project_dwid 
from masterdata_hk.sale_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid isnull
)
select 
distinct  
	a.id,a.activity_dwid,b.property_dwid,b.address_dwid,b.building_dwid,b.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,unit_count,sale_type,sale_subtype,property_completion_year,property_type_code,
	address_unit,address_local_text,tenure_code,bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,
	first_listing_date,last_listing_date,transaction_date,listing_amount,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.sale_listing a
left join correct_idbase b on a.property_dwid = b.property_dwid
where b.property_dwid notnull; --14

call metadata.sp_submit_change_request(549, 'huying');

call metadata.sp_approve_change_request(549, 'huying');

call metadata.sp_merge_change_request(549);



-- done:
-- check sale_listing v.s. midland_sale_listing_map
select a.*
from masterdata_hk.sale_listing a
left join map_hk.midland_sale_listing__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
and data_source = 'hk-midland-listing-sale'
; -- 72

-- change request
create table branch_hk.midland_sale_listing__map_cr_1 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.sale_listing a
left join map_hk.midland_sale_listing__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
	and data_source = 'hk-midland-listing-sale'
; -- 72
-- merge change request
update map_hk.midland_sale_listing__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_sale_listing__map_cr_1 b
where a.activity_dwid = b.activity_dwid; -- 72



-- update 
select metadata.fn_create_change_request(
    'hk-update-property-2022-08-22', 'huying','huying'
); -- 550

call metadata.sp_add_change_table(550::int, 'hk', replace('property', '-', '_'));


insert into branch_hk.property_cr_550
with correct_idbase as (
select distinct st.property_dwid , st.address_dwid , st.building_dwid , st.project_dwid 
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid notnull
)
select 
	p.id,b.property_dwid,b.address_dwid,b.building_dwid,unit_group_dwid,b.project_dwid,property_type_code,
	property_name,address_unit,address_floor_text,address_floor_num,address_stack,address_stack_num,
	ownership_type_code,bedroom_count,bathroom_count,other_room_count,net_floor_area_sqm,gross_floor_area_sqm,
	slug,country_code,is_active,property_display_text,data_source,data_source_id,status_code,'update' as cr_record_action
from masterdata_hk.property p
left join correct_idbase b on p.property_dwid = b.property_dwid
where b.property_dwid notnull; --21


call metadata.sp_submit_change_request(550, 'huying');

call metadata.sp_approve_change_request(550, 'huying');

call metadata.sp_merge_change_request(550);




select metadata.fn_create_change_request(
    'hk-update-rent-listing-2022-08-22', 'huying','huying'
); -- 551

call metadata.sp_add_change_table(551::int, 'hk', replace('rent-listing', '-', '_'));

insert into branch_hk.rent_listing_cr_551
with correct_idbase as (
select distinct p.property_dwid , p.address_dwid , p.building_dwid , p.project_dwid 
from masterdata_hk.rent_listing st 
left join masterdata_hk.property p on p.property_dwid = st.property_dwid 
where p.building_dwid <> st.building_dwid 
and st.project_dwid isnull
)
select distinct  
	a.id,a.activity_dwid,b.property_dwid,b.address_dwid,b.building_dwid,b.project_dwid,a.lot_group_dwid,a.current_lot_group_dwid,
	activity_display_text,listing_status,rent_type,property_type_code,property_subtype,address_unit,address_local_text,
	bathroom_count,bedroom_count,gross_floor_area_sqm,net_floor_area_sqm,land_area_sqm,gross_floor_area_sqm_min,gross_floor_area_sqm_max,
	first_listing_date,last_listing_date,transaction_date,rent_amount_weekly,rent_amount_monthly,
	a.country_code,a.data_uuid,a.data_source_uuid,a.data_source,status_code,listing_agent_dwid,address_lot_number,'update' as cr_record_action
from masterdata_hk.rent_listing a
left join correct_idbase b on a.property_dwid = b.property_dwid
where b.property_dwid notnull; --42

call metadata.sp_submit_change_request(551, 'huying');

call metadata.sp_approve_change_request(551, 'huying');

call metadata.sp_merge_change_request(551);



-- done:

-- check rent_listing v.s. midland_rent_listing_map
select a.*
from masterdata_hk.rent_listing a
left join map_hk.midland_rent_listing__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 42

-- change request
create table branch_hk.midland_rent_listing__map_cr_1 as 
select 
	a.data_uuid , a.activity_dwid , a.property_dwid , a.building_dwid , a.address_dwid , a.project_dwid , 
	null as land_parcel_dwid, null as lot_group_dwid, null as status_code
from masterdata_hk.rent_listing a
left join map_hk.midland_rent_listing__map b on a.data_uuid = b.data_uuid 
where md5(f_prep_dw_id(a.property_dwid)||'__'||f_prep_dw_id(a.address_dwid)||'__'||f_prep_dw_id(a.building_dwid)||'__'||f_prep_dw_id(a.project_dwid)) 
	!= md5(f_prep_dw_id(b.property_dwid)||'__'||f_prep_dw_id(b.address_dwid)||'__'||f_prep_dw_id(b.building_dwid)||'__'||f_prep_dw_id(b.project_dwid)) 
; -- 42

-- merge change request
update map_hk.midland_rent_listing__map a
set property_dwid = b.property_dwid, building_dwid = b.building_dwid, address_dwid = b.address_dwid, project_dwid = b.project_dwid
from branch_hk.midland_rent_listing__map_cr_1 b
where a.activity_dwid = b.activity_dwid; -- 42







