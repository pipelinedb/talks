CREATE TABLE states (abbrev text, name text);
CREATE INDEX idx ON states (abbrev);

CREATE STREAM raw_page_views (payload json);
CREATE STREAM page_views
(
	user_id integer,
	user_agent text,
	method text,
	status integer,
	artist text,
	song text,
	page text,
	name text
);

CREATE CONTINUOUS TRANSFORM normalize_raw_page_views AS
	SELECT
		(payload->>'userId')::integer AS user_id,
		(payload->>'userAgent') AS user_agent,
		(payload->>'method') AS method,
		(payload->>'status')::integer AS status,
		(payload->>'artist') AS artist,
		(payload->>'song') AS song,
		(payload->>'page') AS page,
		s.name
FROM raw_page_views r JOIN states s ON regexp_replace(payload->>'location', '.+,\s+', '') = s.abbrev
THEN EXECUTE PROCEDURE pipeline_stream_insert('page_views');

CREATE CONTINUOUS VIEW page_views_per_s WITH (max_age = '1 minute') AS SELECT COUNT(*) / 60. FROM page_views;
CREATE VIEW page_views_per_s_view AS SELECT * FROM page_views_per_s;

-- Uniques 
CREATE CONTINUOUS VIEW uniques_1hr WITH (max_age = '1 hour') AS
	SELECT COUNT(DISTINCT user_id) FROM page_views;
CREATE VIEW uniques_1hr_view AS SELECT * FROM uniques_1hr;
	
CREATE CONTINUOUS VIEW uniques_1day WITH (max_age = '1 day') AS
	SELECT COUNT(DISTINCT user_id) FROM page_views;
CREATE VIEW uniques_1day_view AS SELECT * FROM uniques_1day;

CREATE CONTINUOUS VIEW uniques_1week WITH (max_age = '1 week') AS
	SELECT COUNT(DISTINCT user_id) FROM page_views;
CREATE VIEW uniques_1week_view AS SELECT * FROM uniques_1week;

CREATE CONTINUOUS VIEW uniques_1month WITH (max_age = '1 month') AS
	SELECT COUNT(DISTINCT user_id) FROM page_views;
CREATE VIEW uniques_1month_view AS SELECT * FROM uniques_1month;

-- Trending songs
CREATE CONTINUOUS VIEW songs_fss_top_20 WITH (max_age = '1 hour') AS
	SELECT fss_agg(artist || ' - ' || song, 20) FROM page_views WHERE song IS NOT null;

CREATE VIEW top_20_songs AS
	SELECT
		unnest(fss_topk_values(fss_agg)::text::text[]) AS song,
		unnest(fss_topk_freqs(fss_agg)::text::integer[]) AS plays
FROM songs_fss_top_20;

-- Response codes
CREATE CONTINUOUS VIEW http_responses AS
	SELECT minute(arrival_timestamp), status, COUNT(*) FROM page_views GROUP BY minute, status;
CREATE VIEW http_responses_view AS SELECT * FROM http_responses;

CREATE STREAM alarm_stream (alarm text);
CREATE function fire_500_alarm() returns trigger AS $$                                                                                                                        
BEGIN
	INSERT INTO alarm_stream (alarm) VALUES ('500 response!');
	return new;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER http_status_alarm AFTER UPDATE ON http_responses FOR EACH ROW
	WHEN (new.status = 500 AND new.count >= 2)
	EXECUTE PROCEDURE fire_500_alarm();

CREATE CONTINUOUS VIEW alarms_last_5min WITH (max_age = '5 minutes') AS
	SELECT alarm FROM alarm_stream;
CREATE VIEW alarms_last_5min_view AS SELECT * FROM alarms_last_5min;

CREATE CONTINUOUS VIEW alarms_last_1hr WITH (max_age = '1 hour') AS SELECT COUNT(*) FROM alarm_stream;
CREATE VIEW alarms_last_1hr_view AS SELECT * FROM alarms_last_1hr;

-- Location map
CREATE CONTINUOUS VIEW page_view_locations AS
	SELECT name, COUNT(*) FROM page_views GROUP BY name;
CREATE VIEW page_view_locations_view AS SELECT * FROM page_view_locations;


-- Make script that fires errors
pipeline_kafka.consume_begin('page_views', 'raw_page_views', format := 'csv', parallelism := 4, delimiter := E'\31', escape := E'\32', quote := E'\30', batchsize := 100000);

CREATE CONTINUOUS VIEW response_by_type WITH (max_age = '1 day') AS
	SELECT
		minute(arrival_timestamp),
		sum(CASE WHEN status = 200 THEN 1 ELSE 0 END) AS status_200,
		sum(CASE WHEN status = 404 THEN 1 ELSE 0 END) AS status_404,
		sum(CASE WHEN status = 500 THEN 1 ELSE 0 END) AS status_500,
		sum(CASE WHEN status = 307 THEN 1 ELSE 0 END) AS status_307
FROM page_views GROUP BY minute;

CREATE VIEW response_by_type_view AS SELECT * FROM response_by_type;
