SELECT
  "PK_Md", 
  "product",
  "reviewDate",
  SUM(CASE WHEN "recommendation" = 'true' THEN 1 ELSE 0 END) AS recommendations
FROM
  "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" IN (
    SELECT "PK_Md"
    FROM "ProductAnalysis"."fctHistory"
)
GROUP BY "PK_Md", "product", "reviewDate"
ORDER BY "PK_Md","reviewDate";