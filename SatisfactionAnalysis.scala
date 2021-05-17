import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SatisfactionAnalysis {
  def main(args:Array[String]): Unit ={
    //Create a SparkConfig object and SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SatisfactionAnalysis")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().master(master = "local").config(conf).getOrCreate()

    //Read in data
    val data_path = "D:\\SparkCode\\src\\main\\resource\\train.csv"
    val df_raw = spark.read.format(source = "csv").option("header","true").load(data_path)

    //Data description and cleaning missing values
    df_raw.show(numRows = 10)
    println("Number of Observation:"+df_raw.count())
    df_raw.describe().show()
    val df = df_raw.na.drop(how = "any")
    df.describe().show()

    //Correction of data types and drop irrelevant columns
    df.printSchema()
    df.printSchema()
    val df_typeready = df.withColumn("Age",df("Age").cast("Int"))
      .withColumn("Flight Distance",df("Flight Distance").cast("Double"))
      .withColumn("Inflight wifi service",df("Inflight wifi service").cast("Int"))
      .withColumn("Departure/Arrival time convenient",df("Departure/Arrival time convenient").cast("Int"))
      .withColumn("Ease of Online booking",df("Ease of Online booking").cast("Int"))
      .withColumn("Gate location",df("Gate location").cast("Int"))
      .withColumn("Food and drink",df("Food and drink").cast("Int"))
      .withColumn("Online boarding",df("Online boarding").cast("Int"))
      .withColumn("Seat comfort",df("Seat comfort").cast("Int"))
      .withColumn("Inflight entertainment",df("Inflight entertainment").cast("Int"))
      .withColumn("On-board service",df("On-board service").cast("Int"))
      .withColumn("Leg room service",df("Leg room service").cast("Int"))
      .withColumn("Baggage handling",df("Baggage handling").cast("Int"))
      .withColumn("Checkin service",df("Checkin service").cast("Int"))
      .withColumn("Inflight service",df("Inflight service").cast("Int"))
      .withColumn("Cleanliness",df("Cleanliness").cast("Int"))
      .withColumn("Departure Delay in Minutes",df("Departure Delay in Minutes").cast("Double"))
      .withColumn("Arrival Delay in Minutes",df("Arrival Delay in Minutes").cast("Double"))
    df_typeready.printSchema()
    val df_ready = df_typeready.drop("_c0").drop("id")
    df_ready.printSchema()

    //Write code into json
    df_ready.write.json("D:\\SparkCode\\src\\main\\resource\\json")
  }
}
