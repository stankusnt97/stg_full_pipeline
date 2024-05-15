
    SELECT 
        w.week_name,
        u.user_alias,
        d.device_name,
        t.hour,
        count(*)/3600 AS time_hour
    FROM {{ source('warehouse', 'fact_stg_nodes') }} base
    JOIN {{ source('warehouse', 'user')}} u
        ON base.user_id = u.user_id
    JOIN {{ source('warehouse', 'week')}} w 
        ON base.week_id = w.week_id
    JOIN {{ source('warehouse', 'time')}} t 
        ON base.time_id = t.time_id
    JOIN {{ source('warehouse', 'device')}} d
        ON base.device_id = d.device_id
    --WHERE base.User = 'U1' AND base.week = 'Week 1'
    GROUP BY 
        w.week_name,
        u.user_alias,
        d.device_name,
        t.hour
    ORDER BY COUNT(*) DESC
        



    SELECT 
        w.week_name,
        u.user_alias,
        t.hour,
        count(*) AS time_hour
    FROM {{ ref('enriched_fact_stg_hourly')}} base
    JOIN {{ ref('enriched_user')}} u
        ON base.user_id = u.user_id
    JOIN {{ ref('enriched_week')}} w 
        ON base.week_id = w.week_id
    JOIN {{ ref('enriched_hour')}} t 
        ON base.time_id = t.time_id
    --WHERE base.User = 'U1' AND base.week = 'Week 1'
    GROUP BY 
        w.week_name,
        u.user_alias,
        t.hour
        
    HAVING count(*) > 1


    SELECT 
        base.total_activity_sec,
        base.total_captured_sec,
        base.hip_misuse_event,
        base.hip_misuse_flag,
        w.week_name,
        u.user_alias,
        t.hour
    FROM {{ ref('enriched_fact_stg_hourly')}} base
    JOIN {{ ref('enriched_user')}} u
        ON base.user_id = u.user_id
    JOIN {{ ref('enriched_week')}} w 
        ON base.week_id = w.week_id
    JOIN {{ ref('enriched_hour')}} t 
        ON base.time_id = t.time_id
    --WHERE base.User = 'U1' AND base.week = 'Week 1'
    