-- Continuous View
CREATE STREAM s0 (x int);
CREATE CONTINUOUS VIEW cv AS
  SELECT x, count(*) FROM s0 GROUP BY x;
INSERT INTO s0 (x)
  SELECT x % 10 FROM generate_series(1, 1000) AS x;

-- Continuous Transform
CREATE STREAM s1 (x int);
CREATE CONTINUOUS TRANSFORM ct AS
  SELECT x FROM s1 WHERE x % 2 = 0
  THEN EXECUTE PROCEDURE pipeline_stream_insert('s0');
INSERT INTO s1 (x)
  SELECT x % 10 FROM generate_series(1, 1000) AS x;

-- Continuous Trigger
CREATE TABLE t (x int, count int);
CREATE FUNCTION f()
RETURNS trigger AS
$$
BEGIN
  INSERT INTO t (x, count) VALUES (NEW.x, NEW."count");
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;
CREATE TRIGGER tg
  AFTER UPDATE ON cv
  FOR EACH ROW WHEN (NEW."count" >= 2000 AND OLD."count" < 2000)
  EXECUTE PROCEDURE f();

-- pipeline_kafka Broker API
SELECT pipeline_kafka.add_broker('localhost:9092');

-- pipeline_kafka Consumer API
CREATE STREAM s (payload json);
CREATE CONTINUOUS VIEW cv AS SELECT count(*) FROM s;
SELECT pipeline_kafka.consume_begin('consumer_topic', 's', parallelism := 2);
SELECT pipeline_kafka.consume_end('consumer_topic', 's');
SELECT pipeline_kafka.consume_begin('consumer_topic', 's', parallelism := 1);
SELECT pipeline_kafka.consume_end('consumer_topic', 's');

-- pipeline_kafka Producer API
CREATE TABLE t (x int, y text);
INSERT INTO t (x, y) SELECT x, md5(random()::text)::text AS y FROM generate_series(1, 100) AS x;
SELECT pipeline_kafka.produce_message('producer_topic', row_to_json(r)::text::bytea) FROM 
  (SELECT x, y FROM t) r;

-- pipeline_kafka Demo
CREATE TABLE team (id int PRIMARY KEY, name text);
CREATE TABLE player (id int PRIMARY KEY, team int REFERENCES team (id), name text);
INSERT INTO team VALUES (1, 'Warriors'), (2, 'Thunder'), (3, 'Cavaliers'), (4, 'Raptors');
INSERT INTO player VALUES 
  (1, 1, 'Stephen Curry'), (2, 1, 'Klay Thompson'),
  (3, 2, 'Kevin Durant'), (4, 2, 'Russel Westbrook'),
  (5, 3, 'LeBron James'), (6, 3, 'Kyrie Irving'),
  (7, 4, 'Kyle Lowry'), (8, 4, 'DeMar DeRozan');
CREATE STREAM ppg_stream_raw (payload json);
CREATE STREAM ppg_stream (id int, name text, team text, points int);
CREATE CONTINUOUS TRANSFORM ppg_enrich AS
  SELECT p.id, p.name, t.name AS team, p.points AS points FROM
    (SELECT p.id, p.name, p.team, (s.payload->>'points')::int AS points FROM ppg_stream_raw AS s JOIN player AS p ON ((s.payload->>'id')::int = p.id)) AS p JOIN 
    team AS t ON (p.team = t.id)
  THEN EXECUTE PROCEDURE pipeline_stream_insert('ppg_stream');
CREATE CONTINUOUS VIEW pts_season_stats AS
  SELECT id, min(name) AS name, min(team) AS team, avg(points), sum(points) AS points, count(*) AS num_games
  FROM ppg_stream GROUP BY id;
CREATE TRIGGER mvp_tg
  AFTER UPDATE ON pts_season_stats
  FOR EACH ROW WHEN (NEW.points >= 1000 AND OLD.points < 1000)
  EXECUTE PROCEDURE pipeline_kafka.emit_tuple('mvp_topic');
SELECT pipeline_kafka.consume_begin('ppg_topic', 'ppg_stream_raw');
SELECT * FROM pts_season_stats ORDER BY sum DESC;
