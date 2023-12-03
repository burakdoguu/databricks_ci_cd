from pyspark.sql.functions import *
from pyspark.sql.types import *


class ProductView:
    def __init__(self):
        self.BOOTSTRAP_SERVER = "pkc-n3603.us-central1.gcp.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "AEQX22MT5LUU66WO"
        self.CLUSTER_API_SECRET = "CcyXf2FM/lxgwUkrvV9ZVV7pYGwB/GCESS/ZOO6aki7fFtlhmMCfogaDo0VG/CLb"


    def getViewSchema(self):
        schema = StructType([
                StructField("event", StringType()),
                StructField("messageid", StringType()),
                StructField("userid", StringType()),
                StructField("properties", StructType([
                    StructField("productid", StringType())])),
                StructField("context", StructType([
                    StructField("source", StringType())]))])

        return schema

    def readViewData(self):
        kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)\
                            .option("kafka.security.protocol", "SASL_SSL") \
                            .option("kafka.sasl.mechanism", "PLAIN") \
                            .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';") \
                            .option("subscribe", "views") \
                            .load()
                
        return kafka_df
    
    def getViewData(self, kafka_df,schema):
        value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"),"timestamp")

        return value_df

    def explodeViewData(self, ViewDf):
        views_explode_df = ViewDf.selectExpr("value.event", "value.messageid", "value.userid",
                                             "value.properties.productid", "value.context.source","timestamp")


        select_views_df = views_explode_df.select(col("event"), col("messageid"), col("userid"),
                                                    col("productid"), col("source"), col("timestamp")).alias("v")

        return select_views_df


    ### Category Data ####
    def readCategoryData(self):
        category_df = spark.read \
            .options(delimiter=",", header=True) \
            .csv("/FileStore/resources/product_category_map.csv")

        return category_df

    def exprCategory(self, categoryDf):
        select_category = categoryDf.select("productid", "categoryid")

        return select_category


    ### View and Category JOIN ###

    def ViewJoinCategory(self, view_df, category_df):
        match_df = view_df.join(category_df.alias("c"), col("v.productid") == col("c.productid"),
                                "inner")

        final_df = match_df.select("event", "messageid", "userid","v.productid", "categoryid", "source","timestamp")

        return final_df


    def upsert(self, view_df, batch_id):        
        view_df.createOrReplaceTempView("views_df_temp_view")
        merge_statement = """MERGE INTO view_category s
                USING views_df_temp_view t
                ON s.productid == t.productid AND s.timestamp == t.timestamp
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """
        view_df._jdf.sparkSession().sql(merge_statement)


    def process(self):
        # View
        schema = self.getViewSchema()
        read_data_kafka = self.readViewData()
        get_view_data_kafka = self.getViewData(read_data_kafka,schema)
        df_view = self.explodeViewData(get_view_data_kafka)

        # Category
        read_data_category = self.readCategoryData()
        df_category = self.exprCategory(read_data_category)

        # Join
        join_df = self.ViewJoinCategory(df_view, df_category)
        sQuery = join_df.writeStream \
                            .queryName("ingest_view_category_data") \
                            .foreachBatch(self.upsert) \
                            .option("checkpointLocation", "/FileStore/resources/chekpoint/final_view_df") \
                            .outputMode("append") \
                            .start()

pw = ProductView()
pw.process()