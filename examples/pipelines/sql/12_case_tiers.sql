-- 12 · CASE expression: bucket orders into tiers, then count per tier.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1,100),(2,500),(3,50),(4,250)) AS t(id, amount);
CREATE INCREMENTAL VIEW tiers AS
  SELECT CASE WHEN amount >= 250 THEN 'high' WHEN amount >= 100 THEN 'mid' ELSE 'low' END AS tier
  FROM orders;
CREATE INCREMENTAL VIEW tier_counts AS SELECT tier, COUNT(*) AS n FROM tiers GROUP BY tier;
CREATE SINK out FROM tier_counts;
START PIPELINE out;
