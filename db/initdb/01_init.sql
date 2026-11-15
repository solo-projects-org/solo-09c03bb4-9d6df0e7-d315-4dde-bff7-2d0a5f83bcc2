-- 震例表：震例号、UTC 发震时刻、纬度、经度、震级
CREATE TABLE seismic_events (
    event_id    text PRIMARY KEY,           -- 震例号
    origin_time timestamptz NOT NULL,       -- UTC 发震时刻
    latitude    numeric(6,3) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude   numeric(7,3) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    magnitude   numeric(3,1) NOT NULL       -- 震级
);

-- 让文本协议输出保持 ISO 风格与 UTC（初始装载与 wal2json 都走文本输出）
ALTER DATABASE quakes SET datestyle = 'ISO, MDY';
ALTER DATABASE quakes SET timezone = 'UTC';

-- 样库：30000 条历史震例，发震时刻分布于 2020-01-01 之后约 2000 天
INSERT INTO seismic_events (event_id, origin_time, latitude, longitude, magnitude)
SELECT 'EQH-' || lpad(g::text, 6, '0'),
       timestamptz '2020-01-01 00:00:00+00'
         + (g * interval '96 minutes')
         + (random() * interval '60 minutes'),
       round((18 + random() * 36)::numeric, 3),
       round((73 + random() * 62)::numeric, 3),
       round((2.5 + random() * random() * 6.8)::numeric, 1)
FROM generate_series(1, 30000) AS g;

CREATE INDEX seismic_events_origin_time_idx ON seismic_events (origin_time);
