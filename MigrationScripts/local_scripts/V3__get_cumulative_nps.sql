SELECT
  "product",
  "reviewDate",
  AVG((SUM(CASE WHEN rating IN (4, 5) THEN 1 ELSE 0 END) - SUM(CASE WHEN rating IN (1, 2) THEN 1 ELSE 0 END)) * 100.0 / COUNT(*)) OVER (PARTITION BY "product" ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg_nps
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE 
  "PK_Md" = 'PLACEHOLDER'
GROUP BY
  "product", "reviewDate"
ORDER BY
  "reviewDate";
