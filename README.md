# python-spark-etl
pyspark etl

# 1 solution Requirements 
To achieve this, please use PySpark and complete the ETL pipline containing the following three methods :
extract - this method should return a Spark dataframe which contains raw data from the input file in input_path.
transform - this method should get a raw dataframe as an input parameter and return a dataframe containing the following tree columns : chargepoint_id, max_duration, avg_duration
load - this method should take this transformed dataframe as input parameter and save the data as parquet format to output path in output_path

ChargePointsETLJob.py


# 2 solution Requirements
Please create the following three functions in the udfs.py file :
get_english_name - this function should get the Species column value and return the English name.
get_start_year - this function should get the Period column value and return the year when data collection began
get_trend - this function should should get the Annual percentage change column value and return the charge trend caategory based on the following rules: 
 + Annual percentage change less than -3.00 - return 'strong decline';
 + Annual percentage change between -3.00 and -0.50(inclusive) - return 'weak decline';
 + Annual percentage change between -0.50 and 0.50(exclusive) - return 'no change';
 + Annual percentage change between 0.50 and 3.00(inclusive) - return 'weak increase';
 + Annual percentage change more than 3.00 - return 'strong increase';

Then, in the saame file udfs.py, please register these functions as PySpark UDF functions (under the same names as Python functions) so the can be used in PySpark.

After doing that, please update transform method inside BirdsETLJob.py. This method accepts a raw dataframe as an Input and should use the functions created above to transform the data to following format : 
species       | category                | collected_from_year | annual_percentage_change  | trend
REED Warbler  | Water and wetland birds | 1981                | 1.72                      | weak increase

The species column should use the get_english_name function and the species column as input.
The collected_from_year column should use the get_start_year function and the Period column as input.
The trend column should use the get_trend function and the Annual percentage change column as input.
Here are the names of the columns that should be included in the dataframe returned by the transform method:
species, category, collected_from_year, annual_percentage_change, trend

BirdsETLJob.py
udfs.py
