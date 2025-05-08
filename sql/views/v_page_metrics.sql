CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_PageMetricsV` AS
SELECT
  -- Convert YYYYMMDD int to DATE
  PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date,

  -- Extract the page_title string from the JSON array under event_params
  (SELECT
     JSON_EXTRACT_SCALAR(param, '$.value.string_value')
   FROM UNNEST(JSON_EXTRACT_ARRAY(event_params, '$.event_params')) AS param
   WHERE JSON_EXTRACT_SCALAR(param, '$.key') = 'page_title'
  ) AS page_title,

  -- Every page_view event counts as 1
  1 AS page_view_count

FROM
  `crystalloids-candidates.Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents`
WHERE
  event_name = 'page_view'
  AND JSON_EXTRACT_SCALAR(
        (SELECT p FROM UNNEST(JSON_EXTRACT_ARRAY(event_params, '$.event_params')) AS p 
         WHERE JSON_EXTRACT_SCALAR(p, '$.key') = 'page_title'
        ), '$.value.string_value'
      ) IS NOT NULL;
