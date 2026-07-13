select count(*) as rownum, '0-raw_payment' as tblnm from hlx_dev_raw.raw_payment union
select count(*) as rownum, '0-raw_loan' as tblnm from hlx_dev_raw.raw_loan union 
select count(*) as rownum, '1.1-lnd_payment' as tblnm from hlx_dev_lnd.lnd_payment union
select count(*) as rownum, '1.2-lnd_loan' as tblnm from hlx_dev_lnd.lnd_loan union
select count(*) as rownum, '1.3-lnd_err_payment' as tblnm from hlx_dev_lnd.lnd_err_payment union
select count(*) as rownum, '1.4-lnd_err_loan' as tblnm from hlx_dev_lnd.lnd_err_loan UNION
select count(*) as rownum, '1.5-lnd_dq_audit' as tblnm from hlx_dev_lnd.lnd_dq_audit UNION
select count(*) as rownum, '2-stg_loan_payment' as tblnm from hlx_dev_stg.stg_loan_payment UNION
select count(*) as rownum, '3.1-mart_delinquency' as tblnm from hlx_dev_mart.mart_delinquency union 
select count(*) as rownum, '3.2-mart_data_observability' as tblnm from hlx_dev_mart.mart_data_observability union
select count(*) as rownum, '3.3-mart_payment_anomaly' as tblnm from hlx_dev_mart.mart_payment_anomaly
order by tblnm;

select * from hlx_dev_lnd.lnd_dq_audit;

select count(*) as rownum, '2-stg_loan_payment' as tblnm from hlx_dev_stg.stg_loan_payment;

select count(*) as rownum, '3.1-mart_delinquency' as tblnm from hlx_dev_mart.mart_delinquency union 
select count(*) as rownum, '3.2-mart_data_observability' as tblnm from hlx_dev_mart.mart_data_observability union
select count(*) as rownum, '3.3-mart_payment_anomaly' as tblnm from hlx_dev_mart.mart_payment_anomaly
order by tblnm;


select loan_id, customer_id, product_type, payment_amount, expected_emi, deviation_pct, loan_status, anomaly_reason
from hlx_dev_mart.mart_payment_anomaly
where loan_status = 'active' 
order by anomaly_reason desc
;


with mart_payment_active as (
    select * from hlx_dev_mart.mart_payment_anomaly
    where loan_status = 'active'
),
mart_payment_non_active as (
    select * from hlx_dev_mart.mart_payment_anomaly
    where loan_status = 'inactive'
)
select 
count(*) as datcount, 
a.product_type as active_prodtype,
n.product_type as non_active_prodtype 
from mart_payment_active a union mart_payment_non_active n 
on a.loan_id = n.loan_id
and a.customer_id = n.customer_id
and a.payment_id = n.payment_id
group by a.product_type, n.product_type
order by a.product_type, n.product_type
;

select product_type,
COUNT(case when loan_status = 'active' then 1 end) as active_count,
COUNT(case when loan_status = 'inactive' then 1 end) as inactive_count
from hlx_dev_mart.mart_payment_anomaly
group by product_type
;
show hlx_dev_mart.mart_payment_anomaly;