### Spotify-ETL

**Description -**

This project demonstrates how the **semi structured data** is extracted from Spotify API and stored is **AWS S3** bucket where it is **triggered by date and events** to transforms it in clean and structured way and save in another S3 bucket. **Crawler** has been created to scan this transformed data and data is saved in **Athena** where SQL functions can be implement for analysis and tabular data is used to create visualization in **AWS Quicksight**

**Architecture-**

![Diagram](https://github.com/shashank-14/Spotify-ETL/blob/main/Spotify%20ETL%20Pipeline.PNG)


**Services used-**

Amazon S3

AWS Lambda

AWS Glue

AWS AthenaQuickSight
