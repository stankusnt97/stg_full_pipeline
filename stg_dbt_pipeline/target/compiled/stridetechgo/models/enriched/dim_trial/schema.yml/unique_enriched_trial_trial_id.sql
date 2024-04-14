
    
    

select
    trial_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_enriched`.`enriched_trial`
where trial_id is not null
group by trial_id
having count(*) > 1


