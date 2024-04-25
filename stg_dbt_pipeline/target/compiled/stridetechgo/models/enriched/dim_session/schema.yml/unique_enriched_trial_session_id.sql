
    
    

select
    session_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_enriched`.`enriched_trial`
where session_id is not null
group by session_id
having count(*) > 1

