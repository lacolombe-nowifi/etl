# Meeting Notes 2019-05-04

## Summary
- No need to use database for the current aim of predictive modeling and analysis (for the sake of experience, we can later try setting up database).
- Modify the scraping script so that it will scrape data, put timestamps, and save the resulting list of dictionaries into a gzip file on S3 (in every 30 seconds).
- Write another script that will daily load the most recent day's gzip file, parse it into a tabular form, and save the result into parquet format on S3.
- Modify the script that cleans past collected raw data such that it saves results into parquet format on S3.
- Try merging the quarterly individual trip data into the RSS feed data (which will help us do network analysis).
- Try deep learning (e.g., LSTM) using the RSS feed data alone.
- Keep exploring how others have approached the prediction problem.
