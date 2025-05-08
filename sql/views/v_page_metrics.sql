CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_PageMetricsV` AS
WITH flat AS (
  SELECT
    -- Parse YYYYMMDD int to DATE
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date,

    -- Extract page_title from the JSON array string
    (SELECT
       JSON_EXTRACT_SCALAR(elem, '$.value.string_value')
     FROM UNNEST(
       JSON_EXTRACT_ARRAY(event_params, '$')
     ) AS elem
     WHERE JSON_EXTRACT_SCALAR(elem, '$.key') = 'page_title'
    ) AS page_title,

    -- Extract page_views (int) and cast it
    CAST(
      (SELECT
         JSON_EXTRACT_SCALAR(elem, '$.value.int_value')
       FROM UNNEST(
         JSON_EXTRACT_ARRAY(event_params, '$')
       ) AS elem
       WHERE JSON_EXTRACT_SCALAR(elem, '$.key') = 'page_view'
      ) AS INT64
    ) AS page_views

  FROM
    `crystalloids-candidates.Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents`
  WHERE
    event_name = 'page_view'
)
SELECT
  *
FROM
  flat
WHERE
  page_title IS NOT NULL;
