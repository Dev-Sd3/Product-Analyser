SELECT
  "product",
  "reviewDate",
  SUM(CASE WHEN "recommendation" = 'true' THEN 1 ELSE 0 END) OVER (ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_recommendations
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE 
  "PK_Md" = 'PLACEHOLDER'
ORDER BY
  "product","reviewDate";
