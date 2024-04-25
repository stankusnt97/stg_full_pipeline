
  
    
        create or replace table `prod`.`curated`.`curated_fact_user_surveys`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    us.self_reported_physical_activity_per_week_hrs,
    us.satisfied_with_activity,
    us.fear_of_falling,
    us.fall_history,
    us.length_of_time_using_walker,
    us.likes_using_walker,
    us.received_walker_training,
    us.description_of_changes_in_DLa,
    us.would_purchase_stg,
    us.how_much_would_you_pay,
    us.would_recommend_stg,
    us.stg_helped_learn_use_walker_better,
    d.device_id,
    u.user_id,
    fac.facility_id,
    current_timestamp() AS last_updated
FROM `prod`.`integrated`.`user_surveys` us
left join `prod`.`curated`.`curated_device` d
    on d.device_name = us.device_name
left join `prod`.`curated`.`curated_user` u
    on u.user_alias = us.user_alias
left join `prod`.`curated`.`curated_facility` fac
    on fac.facility_name = us.facility_name
  