# AWS-Pyspark-ETL-Job
This module performs statistical analysis on the vendor INFY and SBIN dataset

* Overview-Flow-Diagram of what is achieved.

![overview-diagram](https://user-images.githubusercontent.com/35069575/126752531-697a5a1c-f336-4723-a3fa-1c7c52a8ec4d.jpg)

**Following are the detailed requirement of this use case**
1. Create a s3 bucket named <<bucket name>>. Create folders in the bucket as mentioned below.(Manual)
 * Inbound/<vendor>         eg. Inbound/INFY
 * pre-processed/<vendor>   eg. pre-processed/INFY
 * Landing/<vendor>         eg. Landing/INFY
 * Standardized(Partition on Company name)
 * Outbound
 * archive
2. Upload source file to inbound/subfolder folder (Manual)
3. Create a Counter file and Indicator file (Manual)
  Counter file : this file contains the number of rows of a particular files. Vendor will provide this file and you need to compare it using spark dataframe
  Indicator File : is manily used the check the file size. Vendor sends this file and we need to compare the whole accumulated file size with the summarized file size provided to us.
4. Create two sub folders based on file name in preprocess folder (Manual)
5. Create a code to carry out the following steps,
	* Copy the file to Preprocess folders.
	* Carry out the indicator check.
	* Determine the type of file (xls or csv).  If the file is xls, convert the file to csv format. Delimiter will be ‘|’
	* Perform following data quality checks on the file
	* Check if file header matches the required format.
	* Check if the count of records matches with counter file.
	* ‘NULL’ – A particular filed should not have NULL values (Optional)
6. Place the files in the Landing Folder and create Athena tables.
7. Create and place the parquet files in Standardized layer (Partition) and create Athena tables
8. The next layer would be summarized with the summary for every week/company.
   For better understanding, refer below screenshot of what is expected in summazired layer
   ![image](https://user-images.githubusercontent.com/35069575/126756638-6210f023-02e9-4e79-8afe-bfc093c3d032.png)

9. Create a csv from summary and place it in Outbound/Archive.
