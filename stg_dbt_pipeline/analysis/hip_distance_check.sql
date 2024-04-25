
-- Retrieve hip mistakes for users
SELECT 
    --ROUND(hip_distance), 
    --u.user_alias, 
    w.week_name,
    d.device,
    AVG(hip_distance) AS hip_distance,
    SUM(hip_misuse_flag) AS hip_misuse_flags, 
    SUM(hip_vibration_flag) AS hip_vibration_flag, 
    COUNT(1) AS total_captured_sec, 
    SUM(total_misuse_flag) AS total_misuse_flag,
    (SUM(left_misuse_flag) / COUNT(*)) AS left_misuse_percentage,
    (SUM(right_misuse_flag) / COUNT(*)) AS right_misuse_percentage,
    (SUM(hip_misuse_flag) / COUNT(*)) AS hip_misuse_percentage,
    (SUM(total_misuse_flag) / COUNT(*)) AS total_misuse_percentage
FROM {{ ref( 'enriched_fact_stg') }} stg
JOIN {{ ref( 'enriched_user' ) }} u ON 
    u.user_id = stg.user_id
JOIN {{ ref( 'enriched_week' )}} w ON 
    w.week_id = stg.week_id
JOIN {{ ref('enriched_device')}} d ON 
    d.device_id = stg.device_id
WHERE w.week_no >= 7
GROUP BY 
    --1, 
    --u.user_alias, 
    w.week_name,
    d.device
ORDER BY w.week_name DESC



SELECT hip_misuse_flag
FROM {{ ref('enriched_fact_stg' )}}
GROUP BY hip_misuse_flag