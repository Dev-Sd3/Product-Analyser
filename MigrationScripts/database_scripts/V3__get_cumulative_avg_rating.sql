SELECT
  "PK_Md", 
  "product",
  "reviewDate",
  (SUM("rating") OVER (PARTITION BY "product" ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::numeric /
  (COUNT(*) OVER (PARTITION BY "product" ORDER BY "reviewDate" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::numeric AS cumulative_avg_rating
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" IN (
    SELECT "PK_Md"
    FROM "ProductAnalysis"."fctHistory"
)
ORDER BY "PK_Md","reviewDate";