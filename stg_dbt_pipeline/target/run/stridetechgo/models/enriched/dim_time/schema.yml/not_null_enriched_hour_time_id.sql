select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select time_id
from `prod`.`enriched`.`enriched_hour`
where time_id is null



      
    ) dbt_internal_test