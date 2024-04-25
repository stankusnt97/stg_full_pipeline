
    
    

select
    time_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_enriched`.`enriched_hour`
where time_id is not null
group by time_id
having count(*) > 1


