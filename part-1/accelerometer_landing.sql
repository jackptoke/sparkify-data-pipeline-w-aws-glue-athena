CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double)
LOCATION
  's3://stedi-toke-bucket/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json')