CREATE EXTERNAL TABLE `step_trainer_landing`(
  `serialnumber` string COMMENT 'from deserializer',
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  `distancefromobject` int COMMENT 'from deserializer')
LOCATION
  's3://stedi-toke-bucket/step_trainer/landing/'
TBLPROPERTIES (
  'classification'='json')