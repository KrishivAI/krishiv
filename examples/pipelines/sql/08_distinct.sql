-- 08 · Distinct: unique visitor count.
CREATE SOURCE hits AS
  SELECT * FROM (VALUES ('u1'),('u2'),('u1'),('u3'),('u2')) AS t(user_id);
CREATE INCREMENTAL VIEW uniques AS SELECT COUNT(DISTINCT user_id) AS unique_users FROM hits;
CREATE SINK out FROM uniques;
START PIPELINE out;
