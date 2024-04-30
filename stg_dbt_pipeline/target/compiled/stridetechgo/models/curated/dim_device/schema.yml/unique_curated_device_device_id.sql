
    
    

select
    device_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_curated`.`curated_device`
where device_id is not null
group by device_id
having count(*) > 1


