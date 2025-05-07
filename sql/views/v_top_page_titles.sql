CREATE OR REPLACE VIEW `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_TopPagesV` AS
SELECT
  page_title,
  SUM(page_views) AS total_page_views,
  RANK() OVER (ORDER BY SUM(page_views) DESC) AS page_rank
FROM `Mozaffar_Kazemi_GA4Model.Mozaffar_Kazemi_PageMetricsV`
GROUP BY page_title;