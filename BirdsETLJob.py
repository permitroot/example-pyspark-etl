from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from solution.udfs import get_english_name, get_start_year, get_trend

class BirdsETLJob:
    input_path = 'data/input/birds.csv'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("BirdsETLJob")
                                          .getOrCreate())

    def extract(self):
        input_schema = StructType([StructField("Species", StringType()),
                                   StructField("Category", StringType()),
                                   StructField("Period", StringType()),
                                   StructField("Annual_percentage_change", DoubleType())
                                   ])
        return self.spark_session.read.csv(self.input_path, header=True, schema=input_schema)

    def transform(self, df):
        self.spark_session.udf.register("getEnglishName", get_english_name, StringType())
        self.spark_session.udf.register("getStartYear", get_start_year, StringType())
        self.spark_session.udf.register("getTrend", get_trend, StringType())

        df.createOrReplaceTempView("birds")
        df_transformed = self.spark_session.sql("SELECT getEnglishName(Species) AS species, category, \
                            getStartYear(Period) AS collected_from_year, \
                            Annual_percentage_change AS annual_percentage_change, \
                            getTrend(Annual_percentage_change) AS trend FROM birds")

        return df_transformed

    def run(self):
        return self.transform(self.extract())
