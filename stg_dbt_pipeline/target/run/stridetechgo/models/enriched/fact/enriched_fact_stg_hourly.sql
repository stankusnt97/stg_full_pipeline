
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_fact_stg_hourly`
      
      
    using delta
      
      
      
      
      
      
      
      as
      select
    count(1) as total_captured_sec,
    SUM(activity_flag) AS total_activity_sec,
    TRY_CAST(AVG(left_lbf) AS NUMERIC) AS avg_left,
    MAX(left_lbf) AS max_left,
    MIN(left_lbf) AS min_left,
    TRY_CAST(AVG(right_lbf) AS NUMERIC) AS avg_right,
    MAX(right_lbf) AS max_right,
    MIN(right_lbf) AS min_right,
    CAST(AVG(force_threshold) AS DECIMAL(4,2)) AS force_thresh,
    CAST(AVG(hip_distance) AS DECIMAL(4,2)) AS avg_hip_distance,
    CAST(AVG(hip_distance_threshold) AS DECIMAL(4,2)) AS hip_thresh,
    SUM(left_misuse_flag) AS left_misuse_event,
    SUM(right_misuse_flag) AS right_misuse_event,
    SUM(hip_misuse_flag) AS hip_misuse_event,
    SUM(total_misuse_flag) AS total_misuse_event,
    -- Product QA
    SUM(left_missed_trigger) AS left_missed_trigger,
    SUM(right_missed_trigger) AS right_missed_trigger,
    SUM(hip_missed_trigger) AS hip_missed_trigger,
    SUM(total_missed_trigger) AS total_missed_trigger,
    SUM(left_trigger_misfire) AS left_trigger_misfire,
    SUM(right_trigger_misfire) AS right_trigger_misfire,
    SUM(hip_trigger_misfire) AS hip_trigger_misfire,
    SUM(total_trigger_misfire) AS total_trigger_misfire,
    AVG(left_lbf) + STDDEV(left_lbf) AS left_lbf_top_error,
    AVG(left_lbf) - STDDEV(left_lbf) AS left_lbf_bottom_error,
    AVG(right_lbf) + STDDEV(right_lbf) AS right_lbf_top_error,
    AVG(right_lbf) - STDDEV(right_lbf) AS right_lbf_bottom_error,
    AVG(hip_distance) + STDDEV(hip_distance) AS hip_distance_top_error,
    AVG(hip_distance) - STDDEV(hip_distance) AS hip_distance_bottom_error,
    TRY_CAST(AVG(left_adc) AS NUMERIC) AS avg_left_adc,
    AVG(left_adc) + STDDEV(left_adc) AS left_adc_top_error,
    AVG(left_adc) - STDDEV(left_adc) AS left_adc_bottom_error,
    TRY_CAST(AVG(right_adc) AS NUMERIC) AS avg_right_adc,
    AVG(right_adc) + STDDEV(right_adc) AS right_adc_top_error,
    AVG(right_adc) - STDDEV(right_adc) AS right_adc_bottom_error,
    -- Session metrics
    COUNT(user_session_no) AS user_session_count,
    AVG(user_session_length) AS avg_user_session_length,
    TRY_CAST(MAX(user_session_end_time) AS TIMESTAMP) AS latest_user_session_time,
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
    facility_id,
    user_id,
    device_id,
    time_id,
    week_id,
    session_id,
    MAX(extraction_time) AS extraction_time,
    current_timestamp() AS last_updated
from `dev`.`dbt-nstankus_enriched`.`enriched_fact_stg`
group by facility_id, user_id, device_id, time_id, week_id, session_id
  