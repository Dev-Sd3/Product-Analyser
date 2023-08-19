SELECT
  "product",
  "reviewDate",
  (SUM("rating") OVER (PARTITION BY "product" ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::numeric /
  (COUNT(*) OVER (PARTITION BY "product" ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::numeric AS cumulative_avg_rating
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE 
  "PK_Md" = 'PLACEHOLDER'
ORDER BY
  "reviewDate";
