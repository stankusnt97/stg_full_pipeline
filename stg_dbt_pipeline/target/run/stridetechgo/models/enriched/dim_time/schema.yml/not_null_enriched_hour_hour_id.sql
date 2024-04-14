select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select hour_id
from `dev`.`dbt-nstankus_enriched`.`enriched_hour`
where hour_id is null



      
    ) dbt_internal_test