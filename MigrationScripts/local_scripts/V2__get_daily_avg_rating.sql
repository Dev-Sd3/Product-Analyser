SELECT "product", "reviewDate", AVG(rating) AS avg_rating, COUNT(rating) as ratings
FROM "ProductAnalysis"."stgBestbuyReviews"
WHERE "PK_Md" = 'PLACEHOLDER'
GROUP BY "product", "reviewDate"
ORDER BY "reviewDate";