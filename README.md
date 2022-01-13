# DataLoad
A pyspark script for loading incremental data from AMFI
------------
Tools used:
1. Pyspark
2. VM instance on GCP
------------
Flow:
1. The latest date (max_dt) in the local data is checked for incremental load. (Initial date has been set to 01-Jan-1980)
2. TextdData is pulled in blocks of stream from the URL - https://www.amfiindia.com/spages/NAVAll.txt?t=11012022102943
3. Then the fetched data later than the max_dt is filtered
4. Null rows are removed
5. The white spaces in column names is replaced with underscore becasue for writing parquet file white spaces create problem
6. year and month column are added becasue they will be used for creating partitions
7. data is appended to an existing parquet file which is partitioned on year, month and Scheme_Name
------------
Data Model:

![image](https://user-images.githubusercontent.com/97655295/149272570-02e274e3-9559-46c8-b61c-dbee621d2d8d.png)
------------
Sample data:
![image](https://user-images.githubusercontent.com/97655295/149272715-1f0e9958-6e95-48ee-ab5d-0baad3ebdef6.png)
