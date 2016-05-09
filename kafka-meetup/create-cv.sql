CREATE STREAM s (x int, y text);

CREATE CONTINUOUS VIEW cv AS SELECT y, count(*)
  FROM s
  WHERE x > 10
  GROUP BY y;

INSERT INTO s (x, y) SELECT i AS x, (i % 4)::text AS y FROM generate_series(1, 100) AS i;
