import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * @Author: caoguoqing
  * @Date: @2021/1/5 9:01
  *        scala
  *        Content:主要实现了流读取数据和spark ml
  */
object Exapmle02 {


    /**
      * 类
      * @param DEST_COUNTRY_NAME
      * @param ORIGIN_COUNTRY_NAME
      * @param count
      */
    case class Flight(DEST_COUNTRY_NAME:String, ORIGIN_COUNTRY_NAME:String, count:BigInt)
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("Example").getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", "3")
    /*    val fightsDF = spark.read.parquet("E:\\Git\\spark\\data\\flight-data\\parquet\\2010-summary.parquet\\")
        import spark.implicits._
        val fights = fightsDF.as[Flight]
        //val americanFights = fights.filter(ele => ele.ORIGIN_COUNTRY_NAME == "United States").map(ele => ele)
       // americanFights.show(americanFights.count().toInt, false)
        fights.printSchema()
        fights.filter(fight_row => fight_row.ORIGIN_COUNTRY_NAME !="Canada").map(fight_row => fight_row).show(false)
        import org.apache.spark.sql.functions.desc
        fights.select("count","ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").sort(desc("count")).limit(1).show()
        println(fights.count())
        val five = fights.take(5).filter(ele => ele.ORIGIN_COUNTRY_NAME != "Canada").map (ele => Flight(ele.DEST_COUNTRY_NAME,
            ele.ORIGIN_COUNTRY_NAME, ele.count+5)).foreach(ele => println(ele.ORIGIN_COUNTRY_NAME+","+ele.DEST_COUNTRY_NAME+","+ele.count))*/


        val reader = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema","true")
          .load("E:\\Git\\spark\\data\\retail-data\\by-day\\*.csv")


//        reader.createOrReplaceTempView("retail_data")
//
        val staticSchema = reader.schema
//
        import org.apache.spark.sql.functions.{col,window, column,desc}
        reader.selectExpr("CustomerID", "(UnitPrice * Quantity) as total_cost" , "InvoiceDate")
          .groupBy(col("CustomerID"), window(col("InvoiceDate"), "1 day"))
          .sum("total_cost").show(100, false)
        println(staticSchema)

        import spark.implicits._
    /*    val streamReader = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger", 2).format("csv")
          .option("header", "true").load("E:\\Git\\spark\\data\\retail-data\\by-day\\*.csv")
        streamReader.printSchema()

        val purchaseByCustomerPerHour = streamReader.selectExpr("CustomerID", "UnitPrice * Quantity as total_cost", "InvoiceDate")
          .withWatermark("InvoiceDate", "1 day")
          .groupBy($"CustomerID", window($"InvoiceDate", "1 day"))
          .sum("total_cost")

        purchaseByCustomerPerHour.printSchema()
         val query = purchaseByCustomerPerHour.select("CustomerID","window.start","window.end","sum(total_cost)")
           .writeStream
           .format("memory")
           .option("checkpointLocation","./path/to/checkpoint/dir")
           .option("path","./path/to/destination/dir")
           .queryName("customer_purchase")
           .outputMode("complete")
          // .trigger(ProcessingTime("25 seconds"))
           .start()

        query.awaitTermination(10000)
        spark.sql(
            """
              select * from customer_purchase order by `sum(total_cost)` DESC
            """).show()*/

        //见时间戳格式转为星期格式
        import org.apache.spark.sql.functions.date_format
        val value = reader
          .na
          .fill(0)
          .withColumn("day_of_week",date_format($"InvoiceDate","EEEE"))
          .coalesce(5)

        value.show(false)

        //拆分训练集和测试集
        val df_train = value.where("InvoiceDate < '2011-07-01'")
        val df_test = value.where("InvoiceDate > '2011-07-01'")
        println(df_train.count())
        println(df_test.count())

        //将星期转为数字
        import org.apache.spark.ml.feature.StringIndexer
        val indexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")

        //将数字转成独热码
        import org.apache.spark.ml.feature.OneHotEncoder
        val encoder = new OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoder")


        //将特征转为特征向量
        import org.apache.spark.ml.feature.VectorAssembler
        val vector = new VectorAssembler().setInputCols(Array("UnitPrice" ,"Quantity" , "day_of_week_encoder")).setOutputCol("features")

        //将预处理阶段整合
        import org.apache.spark.ml.Pipeline
        val pipeline = new Pipeline().setStages(Array(indexer, encoder, vector))

        val pipelineModel = pipeline.fit(df_train)

        val dataFrame_train = pipelineModel.transform(df_train)

        dataFrame_train.cache()
        //训练算法
        import org.apache.spark.ml.clustering.KMeans
        import org.apache.spark.ml.classification._
        val kMeans = new KMeans().setSeed(1).setMaxIter(10).setK(5)

        //导入训练数据进入模型
        val kMeansModel = kMeans.fit(dataFrame_train)
        //
        kMeansModel.computeCost(dataFrame_train)

        val dataFrame_test = pipelineModel.transform(df_test)

        kMeansModel.computeCost(dataFrame_test)
        //query.stop()
        spark.close()

    }



}
