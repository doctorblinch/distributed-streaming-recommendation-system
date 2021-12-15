package prediction
import org.apache.spark.ml.recommendation.{ALSModel}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, TimestampType, DoubleType}
import org.apache.spark.sql.functions.{from_json,col,current_timestamp, udf, to_timestamp}
import org.apache.spark.sql.SparkSession
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

object StreamingPrediction {

    def main(args: Array[String]): Unit = {

        val bootstrapServers = args(0) // localhost:9191
        val topic = args(1) // blabla
        val modelPath = args(2) // /var/scratch/ddps2107/als.model
        val output = args(3) // console

        val spark = SparkSession
            .builder
            .appName("StreamingPrediction")
            .getOrCreate()

        import spark.implicits._
        
        val schema = StructType(Array(
            StructField("userId",IntegerType, false),
            StructField("movieId",IntegerType, false),
            StructField("timestampGenerator", TimestampType, false)
        ))

        // val m = ALSModel.load("/tmp/ddps2107/als.model")
        val m = ALSModel.load(modelPath)

        val getTimestamp = udf(() =>{
            val now = new Date();  
            val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");  
            val date = timestampFormat.format(now);
            date
        })

        val df = spark.readStream
            .format("kafka")
            // .option("kafka.bootstrap.servers", "node114:9081,node114:9082,node114:9083") 
            .option("kafka.bootstrap.servers", bootstrapServers) 
            .option("subscribe", topic) 
            .option("failOnDataLoss", "false")
            .option("startingOffsets", "earliest") // From starting
            .load()

        val StringDF = df
            .withColumn("timestampSpark", current_timestamp())
            .selectExpr("CAST(value AS STRING)", "timestamp as timestampKafka", "timestampSpark") // Not sure if we need, but to convert binary to string
        
        val userDF = StringDF
            .select(from_json(col("value"), schema).as("data"), col("timestampKafka"), col("timestampSpark"))
            .select("data.*", "timestampKafka", "timestampSpark")

        val pred = m.transform(userDF)
            .withColumn("EventLatency", to_timestamp(getTimestamp()).cast(DoubleType) - col("timestampGenerator").cast(DoubleType))
            .withColumn("ProcessingLatency", to_timestamp(getTimestamp()).cast(DoubleType) - col("timestampSpark").cast(DoubleType))
            .withColumn("ProcessedTimestamp", to_timestamp(getTimestamp()))

        pred.explain();

      
        if (output=="console") {
            pred.writeStream.outputMode("append")
                .format("console")
                .option("truncate", "false").start().awaitTermination()
        }
        else {
            pred.writeStream
                .format("parquet")
                .option("checkpointLocation", "/var/scratch/ddps2107/checkpoint")
                .option("path", "/var/scratch/ddps2107/" + output)
                .start().awaitTermination()
        } 
    }
}
