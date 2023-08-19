SELECT "PK_Md", "product", "reviewDate", COUNT(*) AS entry_count
FROM "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" IN (
    SELECT "PK_Md"
    FROM "ProductAnalysis"."fctHistory"
)
GROUP BY "PK_Md", "product", "reviewDate"
ORDER BY "PK_Md","reviewDate";