# HemaAssessment
This repository contains the code for the assessment for a data engineer position. 
It's an ETL pipeline written in pyspark, for ingesting a dataset from [kaggle]
(https://www.kaggle.com/rohitsahoo/sales-forecasting).  

In order to run it, after building the image, you need to run the container with the following 
file structure mounted to the /dataLake path: 

- dataLake:
  - landing
  - raw
  - consumption 
  - curated

As no requirements where provided about operations, the most simple case is assumed; a daily
process of one file. One file at a time should be put in the landing zone, then the pipeline 
is ready to run. The container is launched with the bash entrypoint, so you need to call the 
main script via "python3 main.py".