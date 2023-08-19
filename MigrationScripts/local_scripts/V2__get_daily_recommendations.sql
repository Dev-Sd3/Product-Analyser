SELECT
  "product",
  "reviewDate",
  SUM(CASE WHEN "recommendation" = 'true' THEN 1 ELSE 0 END) AS recommendations
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE 
  "PK_Md" = 'PLACEHOLDER'
GROUP BY
  "product","reviewDate"
ORDER BY
  "reviewDate";
