drop table fiad.list_as_fba_false_conversion;
create table fiad.list_as_fba_false_conversion
(message_timestamp timestamp,
truncated_message_time timestamp,
encrypted_MerchantId varchar(20));

COPY fiad.list_as_fba_false_conversion
FROM 's3://ipt-temporary-files/false-conversion-merchant-ids-EU-2021-11-21.csv'
credentials 'aws_iam_role=arn:aws:iam::831933027372:role/RedshiftAdminRole'
region 'us-east-1' delimiter ',' ignoreheader 1;
select * from stl_load_errors where filename like '%false-conversion-merchant-ids-EU-2021-11-21%';
select * from fiad.list_as_fba_false_conversion limit 100;

unload ('select * from usr_madg_unknwn_analysis_gcids_ord_src_types_4')
TO 's3://consumer-payments-adhoc/madg/gc_unknown/' credentials 'aws_iam_role=arn:aws:iam::839430550009:role/BusinessS3RedshiftRole';

-------------COMMINGLING STS----------------
select * from fiad.comm_sts_prod_logs_na limit 100;



/*Base query is same as the FBA Inbound WBR query Commingling CCR query. Additional filters have been added based on discussion with Clifford Cho - https://issues.amazon.com/issues/V395886164*/
DROP TABLE IF EXISTS OUTB;
CREATE TEMP TABLE OUTB DISTKEY(order_id) sortkey(asin)AS
(
SELECT
          marketplace_id,
          region_id,
          gl_product_group,
          gl_product_group_desc,
          fcsku,
          fnsku,
          asin,
          platform,
          inventory_owner_group_id,
          merchant_customer_id,
          customer_order_item_id,
          order_id,
          fc_type,
          order_day
FROM
         monster.outbound_fe
WHERE  order_day between sysdate - 360 and sysdate - 42
        AND marketplace_id IN (6)
        AND platform IN ('FBA', 'Retail')
        AND fc_type IN ('Sort', 'NonSort')
GROUP BY
     1,2,3,4,5,6,7,8,9,10,11,12,13,14
);


DROP TABLE IF EXISTS DEFECTS;
CREATE TEMP TABLE DEFECTS DISTKEY(order_id) sortkey(asin)AS
(
SELECT
       merchant_customer_id,
       rate_type,
       defect_type,
       marketplace_id,
       customer_order_item_id,
       asin,
       fulfilled_by,
       gl_product_group,
       has_hard_hit,
       hit_description,
       is_commingled,
       is_retail_merchant,
       merchant_type,
       offering_sku,
       order_id,
       region_id,
       source_iog,
       defect_id,
       source_merchant_customer_id,
       defect_subtype,
       bl_has_hard_hit
FROM
       product_quality_ddl.a_pq_risky_defects
WHERE
     rate_type = 'CCR'
     and nvl(is_valid,'Y')='Y'
     and merchant_type = 'SLR'
     AND  (COALESCE(has_hard_hit, 'N') = 'Y' AND NVL(is_valid, 'Y') = 'Y'
OR (coalesce(defect_status, 'N') = 'verified-stage-1' OR coalesce(defect_status, 'N') = 'verified'))

     AND order_day between sysdate - 360 and sysdate -42
     AND marketplace_id IN (6)

GROUP BY
     1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
);
select * from defects limit 100;
select * from product_quality_ddl.a_pq_risky_defects limit 100;
select max(trunc(order_day)) from product_quality_ddl.a_pq_risky_defects where rate_type = 'CCR' and merchant_type = 'SLR' and nvl(is_valid,'Y')='Y' and nvl(is_valid,'Y')='Y';
select max(order_day) from product_quality_ddl.a_pq_risky_defects where rate_type = 'CCR'
     and nvl(is_valid,'Y')='Y'
     and merchant_type = 'SLR'
     AND  (COALESCE(has_hard_hit, 'N') = 'Y' AND NVL(is_valid, 'Y') = 'Y'
OR (coalesce(defect_status, 'N') = 'verified-stage-1' OR coalesce(defect_status, 'N') = 'verified')) and marketplace_id = 6;

DROP TABLE IF EXISTS OUTB_DEFECTS;
CREATE TEMP TABLE OUTB_DEFECTS DISTKEY(fcsku) sortkey(fcsku)AS
(
SELECT
       o.marketplace_id,
       o.fcsku,
       o.asin,
       o.fnsku,
       o.region_id,
       o.gl_product_group,
       o.gl_product_group_desc,
       o.inventory_owner_group_id AS inventory_owner_group_id_record,
       o.merchant_customer_id AS merchant_customer_id_record,
       o.customer_order_item_id,
       o.fc_type,
       o.platform,
       o.order_day,
       d.merchant_customer_id,
       d.rate_type,
       d.defect_type,
       d.fulfilled_by,
       d.has_hard_hit,
       d.is_commingled,
       d.is_retail_merchant,
       d.merchant_type,
       d.order_id,
       d.defect_id,
       d.source_iog,
       d.source_merchant_customer_id,
       d.bl_has_hard_hit

FROM
      OUTB o INNER JOIN DEFECTS d
       ON o.customer_order_item_id = d.customer_order_item_id
       AND o.marketplace_id = d.marketplace_id
       AND o.ASIN = d.ASIN
);
select * from OUTB_DEFECTS limit 100;



DROP TABLE IF EXISTS OUTB_DEFECTS_PRO;
CREATE TEMP TABLE OUTB_DEFECTS_PRO DISTKEY(asin) sortkey(asin) AS
(
SELECT
          o.*,
          provenance,
          ofpp.inventory_owner_group_id AS inventory_owner_group_id_source,
          dfm.merchant_customer_id AS merchant_customer_id_source
FROM
         OUTB_DEFECTS o
          LEFT JOIN booker.O_FCSKU_PROVENANCE_PROPERTIES ofpp
          ON ofpp.fcsku = o.fcsku and ofpp.region_id = o.region_id
          LEFT JOIN booker.d_fba_marketplace_merchants dfm
          ON dfm.inventory_owner_group_id = ofpp.inventory_owner_group_id
          AND dfm.region_id = ofpp.region_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
);

DROP TABLE IF EXISTS OUTB_DEFECTS_PROVENANCE_CCR_GOAL2;
CREATE TEMP TABLE OUTB_DEFECTS_PROVENANCE_CCR_GOAL2 AS(
SELECT
       *,
       ORDER_ID||ASIN as Identifier,
       CASE
            WHEN FnSku LIKE 'X%' THEN 'Stickered Unit'
            WHEN merchant_customer_id_record IS NULL THEN 'No outbound seller'
            WHEN (merchant_customer_id_record <> merchant_customer_id_source) THEN 'Commingled Unit'
            WHEN (merchant_customer_id_record = merchant_customer_id_source) THEN 'Not Commingled Unit'
            WHEN (fcsku IS NULL) THEN 'FcSku NULL - No virtual tracking'
            WHEN (fcsku NOT LIKE 'ZZ%') AND (FCSKU = FNSKU) THEN 'FcSku = ASIN  - No virtual tracking'
            WHEN (merchant_customer_id_source IS NULL) THEN 'floor items or returned items'
            WHEN (merchant_customer_id_source IS NULL) AND (provenance in ('UNKNOWN','SHARED')) AND (platform = 'FBA') THEN 'Commingled Unit'
            ELSE 'Unknown' END AS if_commingled,
       platform AS plattype_record,
       CASE  WHEN inventory_owner_group_id_source IS NULL AND platform = 'FBA' THEN 'FBA'
             WHEN inventory_owner_group_id_source NOT IN (76673,1,76672,2,3,4,5,261,6,7,8,432014,9,10,11,1069211,12,
                                    13,14,15,16,17,313109,18,19,20,21,22,23,653086,18190,156525,
                                    105205,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,372016,
                                    95373,116870,4262,39,40,41,42,43,44,45,46,47,48,49,50,306,
                                    48179,490687,490686,569,490685,363961,62,1058133,490689,94536,
                                    167914,45394,646234,52058,6880,101,898917,898916,389358,316783,
                                    460636,668301,8176,104691,376,870643,1294676) /*this Seller has huge apparel inbound to SBKG*/
                                             THEN 'FBA'
                ELSE 'Retail' end as plattype_source
FROM
      OUTB_DEFECTS_PRO
);

DROP TABLE IF EXISTS OUTB_DEFECTS_PROVENANCE_CCR_GOAL3;
CREATE TEMP TABLE OUTB_DEFECTS_PROVENANCE_CCR_GOAL3 diststyle key distkey(fcsku) AS(
SELECT
       o.marketplace_id,
       o.fcsku,
       o.asin,
       o.fnsku,
       o.region_id,
       o.gl_product_group,
       o.gl_product_group_desc,
       o.inventory_owner_group_id_record,
       o.merchant_customer_id_record,
       o.customer_order_item_id,
       o.fc_type,
       o.platform,
       o.order_day,
       o.merchant_customer_id,
       o.rate_type,
       o.defect_type,
       o.fulfilled_by,
       o.has_hard_hit,
       o.is_commingled,
       o.is_retail_merchant,
       o.merchant_type,
       o.order_id,
       o.defect_id,
       o.source_iog,
       o.source_merchant_customer_id,
       o.bl_has_hard_hit,
       o.provenance,
       o.inventory_owner_group_id_source,
       o.merchant_customer_id_source,
       o.Identifier,
       o.if_commingled,
       o.plattype_record,
       o.plattype_source,
       CASE WHEN if_commingled = 'Commingled Unit' THEN 'Commingled'
            WHEN if_commingled = 'Not Commingled Unit' THEN 'Not Commingled'
            WHEN if_commingled = 'Stickered Unit' THEN 'Stickered'
            WHEN if_commingled =  'FcSku = ASIN  - No virtual tracking' THEN 'Not Commingled'
            WHEN if_commingled =  'FcSku NULL - No virtual tracking'  THEN 'Not Commingled'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance = 'UNKNOWN' THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance = 'SHARED' THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance IS NULL THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance = 'UNKNOWN' THEN 'Not Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance = 'SHARED' THEN 'Not Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance IS NULL THEN 'Not Commingled - Unknown'
            ELSE if_commingled END AS Result

FROM OUTB_DEFECTS_PROVENANCE_CCR_GOAL2 o
);
select * from OUTB_DEFECTS_PROVENANCE_CCR_GOAL3 limit 100;
select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2 limit 100;
select * from product_quality_ddl_ext.PQ_SELLER_RISK_SCORE_V2 limit 100;
select * from product_quality_ddl_ext.PQ_SELLER_RISK_SCORE_V2 where merchant_customer_id = '106353103'
and marketplace_id = 6;

--merchant risk score
create temp table merchant_score as
SELECT
    merchant_customer_id,
    asin,
    marketplace_id,
    MAX(risk_score) AS sellerAsinRiskScore,
    current_date AS recordDate
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2
    WHERE snapshot_day > current_date - 90 AND snapshot_day < current_date
        AND region_id = 3
        AND marketplace_id IN (6)
    GROUP BY 1,2,3,5

UNION

SELECT
    merchant_customer_id,
    NULL as ASIN,
    marketplace_id,
    MAX(risk_score) AS sellerAsinRiskScore,
    current_date AS recordDate
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_RISK_SCORE_V2
    WHERE snapshot_day > current_date - 90 AND snapshot_day < current_date
        AND region_id = 3
        AND marketplace_id IN (6)
    GROUP BY 1,2,3,5;
select * from merchant_score limit 100;
select merchant_customer_id, asin, count(*) from merchant_score group by 1,2 order by 3 desc limit 100;
select * from merchant_score where asin is null limit 100;
select * from merchant_score where merchant_customer_id = '52529705322';

select merchant_customer_id, count(*) from merchant_score group by 1 order by 2 desc;
select * from merchant_score where merchant_customer_id = '3489916922';


--monthly risk score
drop table if exists merchant_score;
create temp table merchant_score as
SELECT
    base.merchant_customer_id,
    base.asin,
    base.marketplace_id,
    MAX(a.risk_score) AS sellerAsinRiskScore,
    base.snapshot_day
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2 base
left join (select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2
            where region_id = 3 and marketplace_id = 6 and snapshot_day between '2020-10-01' AND '2021-12-31') a
        on base.merchant_customer_id = a.merchant_customer_id
        and base.marketplace_id = a.marketplace_id
        and base.asin = a.asin
        and base.snapshot_day between a.snapshot_day - 90 and a.snapshot_day
    WHERE base.snapshot_day in ('2021-01-01','2021-02-01','2021-03-01','2021-04-01',
    '2021-05-01','2021-06-01','2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01')
    AND base.region_id = 3
    AND base.marketplace_id IN (6)
    GROUP BY 1,2,3,5

UNION

SELECT
    bs.merchant_customer_id,
    NULL as ASIN,
    bs.marketplace_id,
    MAX(aa.risk_score) AS sellerAsinRiskScore,
    bs.snapshot_day
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_RISK_SCORE_V2 bs
left join (select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_RISK_SCORE_V2
            where region_id =3 and marketplace_id = 6 and snapshot_day between '2020-10-01' AND '2021-12-31') aa
        on bs.merchant_customer_id = aa.merchant_customer_id
        and bs.marketplace_id = aa.marketplace_id
        and bs.snapshot_day between aa.snapshot_day - 90 and aa.snapshot_day
    WHERE bs.snapshot_day in ('2021-01-01','2021-02-01','2021-03-01','2021-04-01',
    '2021-05-01','2021-06-01','2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01')
    AND bs.region_id = 3
    AND bs.marketplace_id IN (6)
    GROUP BY 1,2,3,5;





drop table if exists merchant_score2;
create temp table merchant_score2 as
select a.*, dfmm.inventory_owner_group_id
from merchant_score a
left join booker.d_fba_marketplace_merchants dfmm
on a.merchant_customer_id = dfmm.merchant_customer_id
and dfmm.marketplace_id IN (6)
;

select * from merchant_score2 limit 100;
select * from merchant_score2 where merchant_customer_id = '6633062122';
select * from booker.d_fba_marketplace_merchants where merchant_customer_id = '6633062122' limit 100;

select inventory_owner_group_id,count(distinct merchant_customer_id) from merchant_score2 group by 1 order by 2 desc;
select merchant_customer_id,count(distinct inventory_owner_group_id) from merchant_score2 group by 1 order by 2 desc;

select * from booker.d_fba_marketplace_merchants where is_active = 'Y' and inventory_owner_group_id is null
and region_id = 1 limit 100;

drop table awu_check;
create temp table awu_check as
select * from booker.d_fba_marketplace_merchants where merchant_customer_id in (select merchant_customer_id
from merchant_score2 where  inventory_owner_group_id is null)
and marketplace_id = 6;

select count(*) from awu_check;
--227088
select * from awu_check where is_fba_launch = 'Y' limit 100;
--36


/*
--drop before
drop table if exists merchant_score;
CREATE TEMP TABLE merchant_score as
select distinct sts.merchant_id
,sts.fnsku
,sts.trust_score
,dfmm.inventory_owner_group_id
from FIAD.comm_sts_prod_logs_na sts
left join booker.d_fba_marketplace_merchants dfmm
on sts.merchant_id = dfmm.merchant_customer_id
and dfmm.marketplace_id IN (1)
;
select * from merchant_score limit 100;
select count(distinct inventory_owner_group_id), count(distinct merchant_id), count(*) from merchant_score;
select merchant_id, count(distinct trust_score) from merchant_score group by 1 order by 2 desc limit 10;
select * from merchant_score where merchant_id = '40938936305';
select * from FIAD.comm_sts_prod_logs_na limit 100;
select max(trunc(timestamp_log)),min(trunc(timestamp_log)) from FIAD.comm_sts_prod_logs_na;
select * from  booker.d_fba_marketplace_merchants limit 100;
select * from BOOKER.O_FN_SKU_MAPS limit 100;

select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2 limit 100;
select max(trunc(snapshot_day)) from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2 limit 100;
--2021-11-23


drop table if exists merchant_score2;
create temp table merchant_score2 as
    SELECT
    merchant_customer_id,
    marketplace_id,
    asin,
    MAX(risk_score) AS sellerAsinRiskScore
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2
    WHERE snapshot_day > current_date - 90  AND snapshot_day < current_date
        AND region_id = 1
        AND marketplace_id = 1
    GROUP BY 1,2,3
UNION ALL
SELECT
    merchant_customer_id,
    marketplace_id,
    null as asin,
    MAX(risk_score) AS sellerAsinRiskScore
FROM PRODUCT_QUALITY_DDL_ext.PQ_SELLER_RISK_SCORE_V2
    WHERE snapshot_day > current_date - 90 AND snapshot_day < current_date
        AND region_id = 1
        AND marketplace_id = 1
    GROUP BY 1,2,3;
select * from merchant_score2 limit 100;

drop table if exists merchant_score3;
create temp table merchant_score3 as
    select a.*, b.inventory_owner_group_id
from merchant_score2 a
left join booker.d_fba_marketplace_merchants b
on a.merchant_customer_id = b.merchant_customer_id
and b.marketplace_id = 1;
select * from merchant_score3 limit 100;
select count(*) from merchant_score2;
--141,184,413
select count(*) from merchant_score3;
--141,184,413
select count(distinct merchant_customer_id) from merchant_score3;
--5,404,390
select count(distinct merchant_customer_id) from merchant_score3 where inventory_owner_group_id is null;
--74,625,573
--4,163,676

drop table if exists merchant_max;
CREATE temp table merchant_max diststyle key distkey(asin) as
select distinct temp.*
from
(select
merchant_customer_id as merchant_id
,asin
,inventory_owner_group_id
,max(sellerAsinRiskScore) OVER (PARTITION BY merchant_customer_id,asin,inventory_owner_group_id) as max_trust_seller_fnsku
,max(sellerAsinRiskScore) OVER (PARTITION BY merchant_customer_id,inventory_owner_group_id) as max_trust_seller
from merchant_score3 ) temp
where merchant_id is not null
and inventory_owner_group_id is not null
;


drop table if exists merchant_max2;
CREATE temp table merchant_max2 diststyle key distkey(inventory_owner_group_id) as
select distinct temp.*
from
(select distinct
merchant_id
,inventory_owner_group_id
,max_trust_seller
from merchant_max ) temp
where inventory_owner_group_id is not null
;
*/



drop table if exists provenance_trust;
create temp table provenance_trust as (
select outb3.*
,ms1.sellerAsinRiskScore as trust_score1
from OUTB_DEFECTS_PROVENANCE_CCR_GOAL3 outb3
left join merchant_score2 ms1 on outb3.inventory_owner_group_id_source = MS1.inventory_owner_group_id
                                     and extract(month from order_day) = extract(month from snapshot_day)
    AND outb3.asin = MS1.asin
    and MS1.asin is NOT NULL
);
select * from OUTB_DEFECTS_PROVENANCE_CCR_GOAL3 limit 100;
select * from provenance_trust limit 100;
select * from merchant_score2 where inventory_owner_group_id = '52529705322' and asin = 'B073DGH3QP';
select * from merchant_score2 where inventory_owner_group_id = '52529705322' and asin is null;
select * from provenance_trust2 where inventory_owner_group_id_source = '1658155';
select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_ASIN_RISK_SCORE_V2 where merchant_customer_id = '8645309822'
and asin = 'B073DGH3QP';
select * from PRODUCT_QUALITY_DDL_ext.PQ_SELLER_RISK_SCORE_V2 where merchant_customer_id = '52529705322';

drop table if exists provenance_trust2;
create temp table provenance_trust2 as (
select outb3.*
,nvl(trust_score1,ms2.sellerAsinRiskScore) as trust_score2
from provenance_trust outb3
left join merchant_score2 ms2 on outb3.inventory_owner_group_id_source = MS2.inventory_owner_group_id
                                     and extract(month from order_day) = extract(month from snapshot_day)
and ms2.asin is null
);
select * from provenance_trust2 limit 100;
select * from provenance_trust2 where trust_score2 is null ;
select count(*) from provenance_trust2 limit 100;
--24057
select count(*) from provenance_trust2 where trust_score1 is null and trust_score2 is null;
--5746


DROP TABLE IF EXISTS provenance_defect;
CREATE TEMP TABLE provenance_defect AS
(
SELECT
          marketplace_id,
          region_id,
          gl_product_group,
          gl_product_group_desc,
          rate_type,
          Result,
          if_commingled,
          plattype_source,
          plattype_record,
          has_hard_hit,
          trust_score2 as trust_score,
          order_day::date order_day,
          count(distinct order_id) as order_defect_cnt,
          COUNT(distinct ASIN) AS asin_defect_count,
          COUNT(distinct Identifier) AS defect_count,
          COUNT(DISTINCT DEFECT_ID) as defect_id_count
FROM
         provenance_trust2
GROUP BY
         1,2,3,4,5,6,7,8,9,10,11,12
);
select min(order_day), max(order_day) from provenance_defect;
--2021-07-28,2021-11-22


SELECT marketplace_id,
          region_id,
           rate_type,
          Result,
          if_commingled,
          plattype_source,
          plattype_record,
          has_hard_hit,
          order_day,
          case when trust_score >=0.00 and trust_score <= 0.05 then '0.05'
when trust_score <= 0.1 then '0.1'
when trust_score <= 0.15 then '0.15'
when trust_score <= 0.20 then '0.2'
when trust_score <= 0.25 then '0.25'
when trust_score <= 0.3 then '0.3'
when trust_score <= 0.35 then '0.35'
when trust_score <= 0.4 then '0.4'
when trust_score <= 0.45 then '0.45'
when trust_score <= 0.5 then '0.5'
when trust_score <= 0.55 then '0.55'
when trust_score <= 0.6 then '0.6'
when trust_score <= 0.65 then '0.65'
when trust_score <= 0.7 then '0.7'
when trust_score <= 0.75 then '0.75'
when trust_score <= 0.8 then '0.8'
when trust_score <= 0.81 then '0.81'
when trust_score > 0.81 then '0.81+'
else null end as score_bkt,
          sum(order_defect_cnt) as order_defect_cnt,
          sum(asin_defect_count) AS asin_defect_count,
          sum(defect_count) AS defect_count,
          sum(defect_id_count) as defect_id_count
from provenance_defect
group by 1,2,3,4,5,6,7,8,9,10;


select * from booker.d_fba_marketplace_merchants
where inventory_owner_group_id in (1142106)
and marketplace_id IN (3,4,5,35691,44551)

select *
from provenance_trust2 where trust_score2 is null
and inventory_owner_group_id_source is not null
limit 1000
)
and marketplace_id IN (3,4,5,35691,44551)
limit 1000;

select * from provenance_trust2
where inventory_owner_group_id_source = 255243

select distinct merchant_customer_id, inventory_owner_group_id
from booker.d_fba_marketplace_merchants
where merchant_customer_id = 19835673225
and marketplace_id IN (3,4,5,35691,44551)

select * from FIAD.COMM_STS_SHADOW_MODE_LOGS_EU where merchant_id in (19835673225)

select case when inventory_owner_group_id is null then 'missing' else 'available' end as inv_owner_avail, count(*)
from merchant_score group by 1;









--------CCR DENOM----------
DROP TABLE IF EXISTS OUTB;
CREATE TEMP TABLE OUTB DISTKEY(fcsku) sortkey(fcsku)AS
(
SELECT
          marketplace_id,
          region_id,
          gl_product_group,
          gl_product_group_desc,
          fcsku,
          fnsku,
          asin,
          platform,
          inventory_owner_group_id,
          merchant_customer_id,
         -- customer_order_item_id,
         fc_type,
          order_day::date ,
          count(distinct order_id) order_id,
         -- customer_shipment_item_id,
          sum(shipped_units) shipped_units
FROM
         monster.outbound_fe
WHERE
        1 = 1
        and order_day between sysdate - 360 and sysdate - 42
        AND marketplace_id IN (6)
        AND platform IN ('FBA', 'Retail')
        AND fc_type IN ('Sort', 'NonSort')
group by 1,2,3,4,5,6,7,8,9,10,11,12
);

DROP TABLE IF EXISTS OUTB_SWAPS2;
CREATE TEMP TABLE OUTB_SWAPS2 DISTKEY(asin) sortkey(asin) AS
(
SELECT
          marketplace_id,
          o.region_id,
          o.fcsku,
          o.gl_product_group,
          o.gl_product_group_desc,
          --fcsku,
          fnsku,
          asin,
          platform,
          merchant_customer_id,
        --  customer_order_item_id,
        --  customer_shipment_item_id,
          order_id,
          fc_type,
          order_day,
          shipped_units,
          ofpp.inventory_owner_group_id AS inventory_owner_group_id_source,
          ofpp.provenance as provenance,
          o.inventory_owner_group_id AS inventory_owner_group_id_record
FROM
         OUTB o
          LEFT JOIN booker.O_FCSKU_PROVENANCE_PROPERTIES ofpp
          ON ofpp.fcsku = o.fcsku and ofpp.region_id = o.region_id
);

/*CREATE IF_COMMINGLED LABEL BASED ON IOG SOURCE AND RECORD VALUES*/
DROP TABLE IF EXISTS OUTB_SWAPS_GOAL;
CREATE TEMP TABLE OUTB_SWAPS_GOAL AS(
SELECT
       *,
       CASE
            WHEN FnSku LIKE 'X%' THEN 'Stickered Unit'
            WHEN inventory_owner_group_id_record IS NULL THEN 'No outbound seller'
            WHEN (inventory_owner_group_id_record <> inventory_owner_group_id_source) THEN 'Commingled Unit'
            WHEN (inventory_owner_group_id_record = inventory_owner_group_id_source) THEN 'Not Commingled Unit'
            WHEN (fcsku IS NULL) THEN 'FcSku NULL - No virtual tracking'
            WHEN (fcsku NOT LIKE 'ZZ%') AND (FCSKU = FNSKU) THEN 'FcSku = ASIN  - No virtual tracking'
            WHEN (inventory_owner_group_id_source IS NULL) THEN 'floor items or returned items'
            WHEN (inventory_owner_group_id_source IS NULL) AND (provenance in ('UNKNOWN','SHARED')) AND (platform = 'FBA') THEN 'Commingled Unit'
             ELSE 'Unknown' END AS if_commingled,
       platform AS plattype_record,
       CASE  WHEN inventory_owner_group_id_source IS NULL AND platform = 'FBA' THEN 'FBA'
             WHEN inventory_owner_group_id_source NOT IN (76673,1,76672,2,3,4,5,261,6,7,8,432014,9,10,11,1069211,12,
                                    13,14,15,16,17,313109,18,19,20,21,22,23,653086,18190,156525,
                                    105205,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,372016,
                                    95373,116870,4262,39,40,41,42,43,44,45,46,47,48,49,50,306,
                                    48179,490687,490686,569,490685,363961,62,1058133,490689,94536,
                                    167914,45394,646234,52058,6880,101,898917,898916,389358,316783,
                                    460636,668301,8176,104691,376,870643,1294676) /*this Seller has huge apparel inbound to SBKG*/
                                             THEN 'FBA'
                ELSE 'Retail' end as plattype_source
FROM
      OUTB_SWAPS2
);
select * from OUTB_SWAPS_GOAL limit 100;



DROP TABLE IF EXISTS OUTB_SWAPS_GOAL2;
CREATE TEMP TABLE OUTB_SWAPS_GOAL2 diststyle key distkey(asin) AS(
SELECT
       OSG.*,
       CASE WHEN if_commingled = 'Commingled Unit' THEN 'Commingled'
            WHEN if_commingled = 'Not Commingled Unit' THEN 'Not Commingled'
            WHEN if_commingled = 'Stickered Unit' THEN 'Stickered'
            WHEN if_commingled =  'FcSku = ASIN  - No virtual tracking' THEN 'Not Commingled'
            WHEN if_commingled =  'FcSku NULL - No virtual tracking'  THEN 'Not Commingled'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance = 'UNKNOWN' THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance = 'SHARED' THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'FBA' AND  provenance IS NULL THEN 'Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance = 'UNKNOWN' THEN 'Not Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance = 'SHARED' THEN 'Not Commingled - Unknown'
            WHEN if_commingled =  'floor items or returned items'  AND plattype_record = 'Retail' AND  provenance IS NULL THEN 'Not Commingled - Unknown'
            ELSE if_commingled END AS Result


FROM OUTB_SWAPS_GOAL OSG
);
select * from outb_swaps_goal2 limit 100;

drop table if exists join_check;
create temp table join_check diststyle key distkey(inventory_owner_group_id_source) as
SELECT outb3.*
--,ms1.merchant_id
,ms1.sellerAsinRiskScore as trust_score1
--,ms2.max_trust_seller as trust_score2
from
OUTB_SWAPS_GOAL2 outb3
  left JOIN MERCHANT_score2 MS1
    ON outb3.inventory_owner_group_id_source = MS1.inventory_owner_group_id
           and extract(month from order_day) = extract(month from snapshot_day)
    AND outb3.asin = MS1.asin
    and MS1.asin is NOT NULL ;

drop table if exists join_check2;
create temp table join_check2 as
SELECT temp2.*
,nvl(trust_score1,ms2.sellerAsinRiskScore) as trust_score2
from join_check temp2
  left join MERCHANT_score2 MS2
  on temp2.inventory_owner_group_id_source = MS2.inventory_owner_group_id
         and extract(month from order_day) = extract(month from snapshot_day)
  and MS2.inventory_owner_group_id is not null
  and MS2.asin is null
;


DROP TABLE IF EXISTS provenance_Denom;
CREATE TEMP TABLE provenance_Denom AS(
select temp.*
--,coalesce(temp.merchant_id1, ms2.merchant_id) as merchant_id
--,coalesce(temp.trust_score1, ms2.max_trust_seller) as trust_score
from
(select
  marketplace_id,
  merchant_customer_id merchant_id1,
  trust_score2 as trust_score,
  region_id,
  Result,
  if_commingled,
  plattype_record,
  plattype_source,
  provenance,
  inventory_owner_group_id_source,
  order_day::date as order_day,
  sum(order_id) as Ct
  from join_check2
    group by 1,2,3,4,5,6,7,8,9,10,11) temp);

drop table if exists prov_denomt;
create temp table prov_denomt as(
SELECT
marketplace_id,
 -- merchant_customer_id merchant_id1,
--  trust_score,
  region_id,
  Result,
  if_commingled,
  plattype_record,
  plattype_source,
  provenance,
--  inventory_owner_group_id_source,
  order_day::date as order_day,
case when trust_score >=0.00 and trust_score <= 0.05 then '0.05'
when trust_score <= 0.1 then '0.1'
when trust_score <= 0.15 then '0.15'
when trust_score <= 0.20 then '0.2'
when trust_score <= 0.25 then '0.25'
when trust_score <= 0.3 then '0.3'
when trust_score <= 0.35 then '0.35'
when trust_score <= 0.4 then '0.4'
when trust_score <= 0.45 then '0.45'
when trust_score <= 0.5 then '0.5'
when trust_score <= 0.55 then '0.55'
when trust_score <= 0.6 then '0.6'
when trust_score <= 0.65 then '0.65'
when trust_score <= 0.7 then '0.7'
when trust_score <= 0.75 then '0.75'
when trust_score <= 0.8 then '0.8'
when trust_score <= 0.81 then '0.81'
else null end as score_bkt,
sum(ct) as ct
from provenance_denom
group by 1,2,3,4,5,6,7,8,9);


select * from prov_denomt;
select max(order_day) from prov_denomt;

select count(*) from prov_denomt where order_day >= '2021-05-01';


select * from prov_denomt;

  Select sum(Ct),'pro' as flag from  provenance_Denom group by 2
  union all
  Select  count(distinct order_id) as Ct,'osg' as flag  from OUTB_SWAPS_GOAL2 group by 2 ;


SELECT MERCHANT_ID1,
ORDER_DAY,
COUNT(*) CNT
FROM PROVENANCE_DENOM
GROUP BY 1,2
HAVING CNT > 1;


select
fnsku
,inventory_owner_group_id
,count(*)
from merchant_max
group by 1,2
having count(*) > 1

select count(*) from merchant_max limit 1000; where inventory_owner_group_id is null;

select count(*), 'orig' as flag from OUTB_SWAPS_GOAL2 group by 2
union all
select count(*), 'join' as flag from join_check group by 2;

select count(*) from OUTB_SWAPS_GOAL2 limit 1000;




--------------------VT by risk band---------------
DROP TABLE IF EXISTS vt;
CREATE temp TABLE vt DISTKEY(asin) AS
(
SELECT
       vtm.virtual_transfer_id,
       vtm.virtual_transfer_movement_id,
       nwr.fulfillment_reference_id,
       vtm.fulfillment_network_sku AS fnsku,
       vtm.item_authority_id AS ASIN,
       vtm.gl_product_group_id AS gl,
       vtm.quantity AS qty,
       vtm.creation_date,
       vtm.src_inventory_owner_group_id AS iog
FROM
       FBA_VTS.D_VIRTUAL_TRANSFER_MOVEMENTS vtm
        INNER JOIN FBA_VTS.D_NW_COMMINGLING_REQUESTS nwr
        ON vtm.vt_client_request_id = nwr.vt_client_request_id
        AND vtm.region_id = nwr.region_id
WHERE
        3 = (CASE vtm.region_id WHEN 5 THEN 1 ELSE vtm.region_id END)
	       AND   vtm.src_country_code = 'JP' and  vtm.dst_country_code = 'JP'
	       AND   vtm.creation_date between '2021-10-01' and '2021-12-31'
	       AND   vtm.client_name = 'NetworkCommingling' AND   vtm.status_name = 'TRANSFER_COMPLETE'
	       AND   3 = (CASE nwr.region_id WHEN 5 THEN 1 ELSE nwr.region_id END)
GROUP BY
         1,2, 3,4,5,6,7,8,9
);



DROP TABLE IF EXISTS vtm;
CREATE temp TABLE vtm DISTKEY(asin) AS
(
SELECT
       vt1.virtual_transfer_id,
       vt1.virtual_transfer_movement_id,
       vt1.fulfillment_reference_id,
       vt1.fnsku,
       vt1.ASIN,
       vt1.qty,
       vt1.creation_date,
       vt1.iog AS iog1,
       vt2.iog AS iog2
FROM
       vt vt1
        INNER JOIN vt vt2
        ON  vt1.virtual_transfer_id = vt2.virtual_transfer_id
         /* don't use != as it would result double the rows with incorrect values */
        AND vt1.virtual_transfer_movement_id < vt2.virtual_transfer_movement_id
);
select * from vtm limit 100;
select * from vtm where virtual_transfer_id = '9710731651.00000000';
select * from vtm where iog1 = '132474.00000000' and iog2 = '507941.00000000' and asin = 'B0838RWH7Y';
select * from vt where virtual_transfer_id = '9710731651.00000000';




drop table if exists vtm_score;
create temp table vtm_score as
    select a.*, nvl(b.sellerAsinRiskScore,c.sellerasinriskscore) as riskscore
from vtm a
left join (select * from merchant_score2 where asin is not null ) b
on a.iog1 = b.inventory_owner_group_id
and a.asin = b.asin
left join (select * from merchant_score2 where asin is null) c
on a.iog1 = c.inventory_owner_group_id
;
select * from vtm_score  limit 100;
select count(*) from vtm;
--151782795
select count(*) from vtm_score;
--151782795




/* Units transferred successfully */
DROP TABLE IF EXISTS virtual_unit;
CREATE temp TABLE virtual_unit  AS
(
SELECT
       trunc(creation_date) AS creation_date
		  ,CASE WHEN iog1 IN (1, 2,  12,  14,   30,  37,  51) THEN 'Retail' ELSE 'FBA' END AS plattype
		  ,'COMM'  AS type
      ,riskscore as trust_score
      ,SUM(qty) AS vt_unit
FROM
       --vtm
        vtm_score
GROUP BY
        1, 2, 3,4
);


/*
DROP TABLE IF EXISTS outbound_unit;
CREATE temp TABLE outbound_unit  AS
(
SELECT
          ship_day,
          marketplace_id,
          platform,
          CASE
              WHEN fnsku NOT LIKE 'X%' THEN 'COMM'
              ELSE 'STICKER'
           END AS type,
           SUM(quantity_shipped) AS out_unit
FROM
           monster.outbound_na
WHERE
           marketplace_id =1
           AND ship_day >= current_date-180
           AND platform IN ('FBA', 'Retail')
GROUP BY
           1, 2, 3, 4
);


drop table if exists ob_prep;
create temp table ob_prep as
    select a.ship_day, a.marketplace_id, a.platform, a.fnsku, a.quantity_shipped, a.merchant_customer_id, a.inventory_owner_group_id,
           case when b.merchant_id is not null then 'Y' else null end as block
from (select * from monster.outbound_na WHERE marketplace_id =1 AND ship_day >= current_date-270 AND platform IN ('FBA', 'Retail')) a
left join (select * from merchant_blk where fnsku is not null and fnsku != '') b
on a.merchant_customer_id = b.merchant_id
and a.inventory_owner_group_id = b.inventory_owner_group_id
and a.fnsku = b.fnsku;
select block, count(*) from vtm_blc1 group by 1;

drop table if exists ob_prep2;
create temp table ob_prep2 as
    select a.*, nvl(a.block, case when b.merchant_id is not null then 'Y' else null end) as block2
from ob_prep a
left join (select * from merchant_blk where fnsku is null or fnsku = '') b
on a.merchant_customer_id = b.merchant_id
and a.inventory_owner_group_id = b.inventory_owner_group_id
;

DROP TABLE IF EXISTS outbound_unit;
CREATE temp TABLE outbound_unit  AS
(
SELECT
          ship_day,
          marketplace_id,
          platform,
          CASE
              WHEN fnsku NOT LIKE 'X%' THEN 'COMM'
              ELSE 'STICKER'
           END AS type,
       block2,
           SUM(quantity_shipped) AS out_unit
FROM
           ob_prep2
GROUP BY
           1, 2, 3, 4,5
);
select * from outbound_unit limit 100;
select * from virtual_unit limit 100;
select sum(vt_unit) from virtual_unit;
select sum(out_unit) from outbound_unit;
*/

--create temp table chk as
SELECT
       creation_date,
       case when trust_score >=0.00 and trust_score <= 0.05 then '0.05'
        when trust_score <= 0.1 then '0.1'
        when trust_score <= 0.15 then '0.15'
        when trust_score <= 0.20 then '0.2'
        when trust_score <= 0.25 then '0.25'
        when trust_score <= 0.3 then '0.3'
        when trust_score <= 0.35 then '0.35'
        when trust_score <= 0.4 then '0.4'
        when trust_score <= 0.45 then '0.45'
        when trust_score <= 0.5 then '0.5'
        when trust_score <= 0.55 then '0.55'
        when trust_score <= 0.6 then '0.6'
        when trust_score <= 0.65 then '0.65'
        when trust_score <= 0.7 then '0.7'
        when trust_score <= 0.75 then '0.75'
        when trust_score <= 0.8 then '0.8'
        when trust_score <= 0.81 then '0.81'
        else null end as score_bkt,
       plattype,
       type,
       --SUM(out_unit) AS outbound_qty,
       SUM(vt_unit)  AS vt_qty
       -- ,sum(vt_unit)/sum(out_unit) as vt_rate
       -- ,ship_day
FROM
       --outbound_unit ob
       --LEFT JOIN
           virtual_unit vt
       --ON creation_date = ship_day
       --AND platform = plattype
       --AND ob.type = vt.type
--and nvl(ob.block2,'0') = nvl(vt.block2,'0')
--where year = 2021
GROUP BY
       1, 2, 3, 4
;
select sum(vt_qty) from chk;







select count(*) from fba_inbound.comm_asin_blacklist_eu;
--15,955,675
select count(*) from fba_inbound.comm_asin_blacklist_jp;
--9,456,466
select count(*) from fba_inbound.comm_asin_blacklist_na;
--53,493,699
select count(*) from fba_inbound.dnca_duplicates;
--3,186,894,848
select * from fba_inbound.comm_gl_subcat_blacklist_eu;
--152,161
select * from fba_inbound.comm_gl_subcat_blacklist_jp;
--118,159
select * from fba_inbound.comm_gl_subcat_blacklist_na;
--163,197

select * from fba_inbound.comm_gl_subcat_blacklist_jp limit 100;

select * from monster.inbound limit 100;