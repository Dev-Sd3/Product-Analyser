SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'ProductAnalysis' 
  AND table_name = 'stgRedditPosts';
