CREATE EXTERNAL TABLE IF NOT EXISTS `dimdb`.`step_trainer_trusted` (
  `sensorReadingTime` bigint,
  ` serialNumber` string,
  ` distanceFromObject` string,
  `email` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dim-lake-house/step_trainer/step_trainer_trusted/'
TBLPROPERTIES ('classification' = 'json');