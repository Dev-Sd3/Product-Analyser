SELECT
  "product","reviewDate",
  ((SUM(CASE WHEN rating IN (4, 5) THEN 1 ELSE 0 END) - SUM(CASE WHEN rating IN (1, 2) THEN 1 ELSE 0 END)) * 100.0 / COUNT(*)) AS daily_nps
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE 
  "PK_Md" = 'PLACEHOLDER'
GROUP BY
  "product","reviewDate"
ORDER BY
  "reviewDate";
