from pyspark.sql import SparkSession

class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("ElectricChargePointsETLJob")
                                          .getOrCreate())

    def extract(self):
        return self.spark_session.read.csv(self.input_path, header=True).select('CPID', 'PluginDuration')

    def transform(self, df):
        changedTypedf = df.withColumn("chargepoint_id", df["CPID"].cast("string")).withColumn("duration", df["PluginDuration"].cast("double"))
        changedTypedf.createOrReplaceTempView("chargepoint")
        return self.spark_session.sql("select chargepoint_id, round(max(duration), 2) AS max_duration, round(avg(duration),2) AS avg_duration from chargepoint group by chargepoint_id")

    def load(self, df):
        df.write.parquet(self.output_path)

    def run(self):
        self.load(self.transform(self.extract()))
