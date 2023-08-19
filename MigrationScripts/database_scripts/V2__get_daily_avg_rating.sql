SELECT "PK_Md", "product", "reviewDate", AVG(rating) AS avg_rating
FROM "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" IN (
    SELECT "PK_Md"
    FROM "ProductAnalysis"."fctHistory"
)
GROUP BY "PK_Md", "product", "reviewDate"
ORDER BY "PK_Md","reviewDate";