CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_DescriptiveV` AS
SELECT
  PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date,
  COUNTIF(event_name = 'page_view') AS page_views,
  COUNT(DISTINCT user_pseudo_id)  AS active_users,
  COALESCE(
    APPROX_QUANTILES(event_value_in_usd, 2)[OFFSET(1)],
    0
  ) AS median_value_usd
FROM `crystalloids-candidates.Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents`
GROUP BY event_date;
