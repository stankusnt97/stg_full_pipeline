
  
    
        create or replace table `prod`.`curated`.`curated_user`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    user_id,
    user_alias,
    physical_therapist_name,
    walker_purchased_from,
    walker_manufacturer,
    self_reported_activity_level,
    satisfied_with_activity,
    fear_of_falling,
    fall_history,
    length_of_time_using_walker,
    length_of_time_using_walker_units,
    likes_using_walker,
    received_walker_training,
    description_of_changes,
    would_purchase,
    would_pay_how_much,
    would_recommend,
    improved_walker_use,
    file_path,
    extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`processed`.`user` f
  