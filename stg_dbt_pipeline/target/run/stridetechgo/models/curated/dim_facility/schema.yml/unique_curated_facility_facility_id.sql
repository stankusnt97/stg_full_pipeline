select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    facility_id as unique_field,
    count(*) as n_records

from `dev`.`dbt-nstankus_curated`.`curated_facility`
where facility_id is not null
group by facility_id
having count(*) > 1



      
    ) dbt_internal_test