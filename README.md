# athena-with-java
This project demonstrates how to work with Athena using AWS SDK using Java.
The common pattern used is:
- Submit a query
- Keep polling for the query execution to complete
- Retrieve the results

The data files used are comma-separated-value files. The code can be modified 
to work with json files. Note: as of today, Athena support csv, json and Apache
Parquet only supports CSV output.
