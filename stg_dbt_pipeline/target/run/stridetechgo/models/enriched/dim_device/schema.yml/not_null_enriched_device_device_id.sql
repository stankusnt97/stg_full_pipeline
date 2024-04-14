select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select device_id
from `dev`.`dbt-nstankus_enriched`.`enriched_device`
where device_id is null



      
    ) dbt_internal_test