SELECT 
    device_time,
    -- Metrics definition
    CASE 
        WHEN left_lbf > force_threshold THEN 1
            WHEN left_lbf IS NOT NULL THEN 0
            ELSE NULL
    END AS left_misuse_flag,
    CASE 
        WHEN right_lbf > force_threshold THEN 1
            WHEN right_lbf IS NOT NULL THEN 0
            ELSE NULL
    END AS right_misuse_flag,
    CASE 
        WHEN hip_distance > hip_distance_threshold THEN 1
            WHEN hip_distance IS NOT NULL THEN 0
            ELSE NULL
    END AS hip_misuse_flag,
    CASE 
        WHEN (left_lbf > force_threshold OR right_lbf > force_threshold OR hip_distance > hip_distance_threshold) THEN 1
            WHEN COALESCE(left_lbf, right_lbf, hip_distance) IS NOT NULL THEN 0
            ELSE NULL
    END AS total_misuse_flag,
    -- Imposing range constraints on lbf
    CASE 
        WHEN left_lbf < 0 THEN 0
        WHEN left_lbf > 100 THEN 100
        ELSE left_lbf
    END AS left_lbf,
    CASE 
        WHEN right_lbf < 0 THEN 0
        WHEN right_lbf > 100 THEN 100
        ELSE right_lbf
    END AS right_lbf,
    left_adc,
    right_adc,
    hip_distance,
    left_vibration_trigger,
    right_vibration_trigger,
    hip_vibration_trigger,
    force_threshold,
    hip_distance_threshold,
    baseline_left_adc,
    baseline_right_adc,
    left_adc_change,
    right_adc_change,
    accelerometer_motion_flag,
    facility_id,
    device_id,
    time_id,
    week_id,
    user_id,
    session_id,
    file_path,
    extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`processed`.`fact_stg_nodes`