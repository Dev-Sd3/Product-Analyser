SELECT "product", "reviewDate", COUNT(*) AS entry_count
FROM "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" = 'PLACEHOLDER'
GROUP BY "product", "reviewDate"
ORDER BY "reviewDate";
