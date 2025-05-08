CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_TopPagesV` AS
SELECT
  page_title,
  SUM(page_view_count)      AS total_page_views,
  RANK() OVER (ORDER BY SUM(page_view_count) DESC) AS page_rank
FROM
  `crystalloids-candidates.Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_PageMetricsV`
GROUP BY
  page_title;
