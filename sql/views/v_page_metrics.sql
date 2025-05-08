CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_PageMetricsV` AS
WITH flat AS (
  SELECT
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date,
    (SELECT value.string_value
       FROM UNNEST(event_params)
      WHERE key = 'page_title')                AS page_title,
    (SELECT value.int_value
       FROM UNNEST(event_params)
      WHERE key = 'page_view')                 AS page_views
  FROM `crystalloids-candidates.Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents`
  WHERE event_name = 'page_view'
)
SELECT *
FROM flat
WHERE page_title IS NOT NULL;
