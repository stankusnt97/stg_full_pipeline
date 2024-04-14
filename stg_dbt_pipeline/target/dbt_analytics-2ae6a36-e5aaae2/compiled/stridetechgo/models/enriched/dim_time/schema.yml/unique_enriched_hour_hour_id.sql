
    
    

select
    hour_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_enriched`.`enriched_hour`
where hour_id is not null
group by hour_id
having count(*) > 1


