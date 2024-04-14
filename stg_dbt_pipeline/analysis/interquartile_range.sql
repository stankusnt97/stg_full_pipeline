SELECT
    left_quartile,
    COUNT(left_quartile),
	MAX(left_lbf) AS left_lbf_quartile_break
FROM(
	SELECT
        device_id,
        device_time,
        left_lbf,
        right_lbf,
        NTILE(4) OVER (ORDER BY left_lbf) AS left_quartile,
        NTILE(4) OVER (ORDER BY right_lbf) AS right_quartile
	FROM {{ref('enriched_fact_stg')}} f
    JOIN {{ref('enriched_hour')}} h 
        ON f.hour_id = h.hour_id
    WHERE h.week = 'Week 3'
    ORDER BY left_lbf DESC) AS quartiles
GROUP BY left_quartile


SELECT
    right_quartile,
    MAX(right_lbf) AS right_lbf_quartile_break
FROM(
	SELECT
        user_id,
        right_lbf,
        NTILE(4) OVER (ORDER BY right_lbf) AS right_quartile
	FROM {{ref('enriched_fact_stg')}}) AS quartiles
GROUP BY right_quartile


-- Check when != 100
SELECT
    user_id,
    right_lbf,
    NTILE(4) OVER (ORDER BY right_lbf) AS right_quartile
FROM {{ref('enriched_fact_stg')}}
WHERE right_lbf != 100
ORDER BY right_lbf DESC

SELECT count(left_lbf)
FROM {{ref('enriched_fact_stg')}}



SELECT
    user,
    week,
    left_quartile,
    MAX(left_lbf) AS left_lbf_quartile_break,
    right_quartile,
    MAX(right_lbf) AS right_lbf_quartile_break
FROM(
	SELECT
        user,
        week,
        left_lbf,
        right_lbf,
        NTILE(4) OVER (ORDER BY left_lbf) AS left_quartile,
        NTILE(4) OVER (ORDER BY right_lbf) AS right_quartile
	FROM {{ref('curated_fact_stg')}}
    WHERE activity_flag = 1) AS quartiles
WHERE left_quartile = 3 OR right_quartile = 3
GROUP BY user, week, left_quartile, right_quartile


SELECT
    user,
    user_time,
    right_quartile,
    MAX(right_lbf) AS right_lbf_quartile_break
FROM(
	SELECT
        user,
        user_time,
        right_lbf,
        NTILE(4) OVER (ORDER BY right_lbf) AS right_quartile
	FROM {{ref('curated_fact_stg')}}
    WHERE activity_flag = 1) AS quartiles
WHERE right_quartile = 3
GROUP BY user, user_time, right_quartile
ORDER BY right_lbf_quartile_break DESC