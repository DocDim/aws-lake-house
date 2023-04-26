# Project: Data Lakehouse

This project aim to use Step Trainer and mobile app data to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:

- Python and Spark
- AWS Glue
- AWS Athena
- AWS S3

## Workflow Environment Configuration
We'll be creating Python scripts using AWS Glue and Glue Studio. These web-based tools and services contain multiple options for editors to write or generate Python code that uses PySpark.

## Project Data
There are three JSON data sources to use from the Step Trainer. You can download the data from https://video.udacity-data.com/topher/2022/June/62be2ed5_stedihumanbalanceanalyticsdata/stedihumanbalanceanalyticsdata.zip or you can extract it from their respective public S3 bucket locations:

### Customer Records (from fulfillment and the STEDI website):
AWS S3 Bucket URI - s3://cd0030bucket/customers/
contains the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

###Step Trainer Records (data from the motion sensor):

AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

contains the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject
### Accelerometer Records (from the mobile app):

AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

contains the following fields:

- timeStamp
- user
- x
- y
- z


## Files in the Repository
### Python script for data processing
accelerometer_landing_to_trusted_zone.py
customer_landing_to_trusted.py
customer_trusted_to_curated.py
machine_learning_curated.py
step_trainer_landing_to_trusted.py
### SQL script for Data Modeling
Contain sql script for the creation of Table in 
accelerometer_landing.sql
accelerometer_trusted.sql
customer_curated.sql
customer_landing.sql
step_trainer_landing.sql
step_trainer_trusted.sql
