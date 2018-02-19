DROP FUNCTION IF EXISTS public.utmzen2bcalb(zone int4, easting numeric, northing numeric);

CREATE FUNCTION public.utmzen2bcalb(zone int4, easting numeric, northing numeric) RETURNS geometry
AS $$ SELECT ST_Transform(ST_PointFromText('POINT (' || $2 || ' ' || $3 || ')', 32600 + $1), 3005)$$
LANGUAGE SQL;


