SELECT
    report_date,
    region,
    impressions,
    clicks,
    purchases
FROM {{ ref('rpt_daily_kpis') }}
WHERE clicks > impressions
   OR purchases > clicks
