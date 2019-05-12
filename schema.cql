CREATE KEYSPACE IF NOT EXISTS demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE demo.stocks (
  symbol text,
  value  decimal,
  date   timestamp,
  PRIMARY KEY ((symbol), date)
) WITH CLUSTERING ORDER BY (date DESC)
AND default_time_to_live = 86400
AND compaction = {
   'class' : 'TimeWindowCompactionStrategy',
   'compaction_window_unit' : 'MINUTES',
   'compaction_window_size' : 60
};
