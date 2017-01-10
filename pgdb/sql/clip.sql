-- clip one table by another
-- probably better as a plsql function but this works
CREATE TABLE $out_table AS
SELECT $columns
, CASE
  WHEN ST_Within(a.geom, b.geom) THEN a.geom
  ELSE ST_Intersection(ST_Force_2D(ST_MakeValid(a.geom)),
                       ST_Force_2D(ST_MakeValid(b.geom)))
END as geom
FROM $in_table a, $clip_table b
WHERE ST_Intersects(a.geom, b.geom) AND a.geom && grd.geom