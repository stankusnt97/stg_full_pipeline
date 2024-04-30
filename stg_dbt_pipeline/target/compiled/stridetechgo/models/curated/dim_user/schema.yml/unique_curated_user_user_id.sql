
    
    

select
    user_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_curated`.`curated_user`
where user_id is not null
group by user_id
having count(*) > 1


