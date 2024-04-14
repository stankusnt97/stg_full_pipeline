
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_fact_stg_hourly`
      
      
    using delta
      
      
      
      
      
      
      
      as
      select
    count(1) as total_captured_sec,
    CAST(SUM(activity_sec) AS NUMERIC) AS total_activity_sec,
    TRY_CAST(AVG(left_lbf) AS NUMERIC) AS avg_left,
    MAX(left_lbf) AS max_left,
    MIN(left_lbf) AS min_left,
    TRY_CAST(AVG(right_lbf) AS NUMERIC) AS avg_right,
    MAX(right_lbf) AS max_right,
    MIN(right_lbf) AS min_right,
    CAST(AVG(force_threshold) AS DECIMAL(4,2)) AS force_thresh,
    CAST(AVG(hip_distance) AS DECIMAL(4,2)) AS avg_hip_distance,
    MAX(hip_distance) AS max_hip_distance,
    MIN(hip_distance) AS min_hip_distance,
    CAST(AVG(hip_distance_threshold) AS DECIMAL(4,2)) AS hip_thresh,
    SUM(left_misuse_flag) AS left_misuse_event,
    SUM(right_misuse_flag) AS right_misuse_event,
    SUM(hip_misuse_flag) AS hip_misuse_event,
    SUM(total_misuse_flag) AS total_misuse_event,
    -- PT metrics
    MAX(start_abc_score) AS start_abc_score,
    MAX(start_tug_score_fastest_attempt) AS start_tug_score_fastest_attempt,
    MAX(start_tandem_balance_total_time) AS start_tandem_balance_total_time,
    MAX(start_successful_tandem_balance_positions) AS start_successful_tandem_balance_positions,
    MAX(start_functional_reach) AS start_functional_reach,
    MAX(end_abc_score) AS end_abc_score,
    MAX(end_tug_score_fastest_attempt) AS end_tug_score_fastest_attempt,
    MAX(end_tandem_balance_score_total) AS end_tandem_balance_score_total,
    MAX(end_successful_tandem_balance_positions) AS end_successful_tandem_balance_positions,
    MAX(end_functional_reach) AS end_functional_reach,
    -- Survey metrics
    MAX(self_reported_physical_activity_per_week_hrs) AS self_reported_physical_activity_per_week_hrs,
    MAX(satisfied_with_activity) AS satisfied_with_activity,
    MAX(fear_of_falling) AS fear_of_falling,
    MAX(fall_history) AS fall_history,
    MAX(length_of_time_using_walker) AS length_of_time_using_walker,
    MAX(likes_using_walker) AS likes_using_walker,
    MAX(received_walker_training) AS received_walker_training,
    MAX(description_of_changes_in_DLa) AS description_of_changes_in_DLa,
    MAX(would_purchase_stg) AS would_purchase_stg,
    MAX(how_much_would_you_pay) AS how_much_would_you_pay,
    MAX(would_recommend_stg) AS would_recommend_stg,
    MAX(stg_helped_learn_use_walker_better) AS stg_helped_learn_use_walker_better,
    user_id,
    device_id,
    hour_id,
    trial_id,
    -- Adding adc values
    TRY_CAST(AVG(left_adc) AS NUMERIC) AS avg_left_adc,
    MAX(left_adc) AS max_left_adc,
    MIN(left_adc) AS min_left_adc,
    TRY_CAST(AVG(right_adc) AS NUMERIC) AS avg_right_adc,
    MAX(right_adc) AS max_right_adc,
    MIN(right_adc) AS min_right_adc,
    MAX(extraction_time) AS lastupdated
from `dev`.`dbt-nstankus_enriched`.`enriched_fact_stg`
group by hour_id, trial_id, user_id, device_id
  