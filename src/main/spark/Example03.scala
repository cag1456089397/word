import org.apache.avro.generic.GenericData
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/**
  * @Author: caoguoqing
  * @Date: @2021/1/13 10:40
  *        scala
  *        Content:
  */
object Example03 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("Example03").getOrCreate()

//        val frame = spark.range(50).toDF("number")
//
//        frame.select(frame.col("number")+10).show(frame.count().toInt, false)
//
//       val rows = spark.range(2).toDF().collect()
//
//        rows.foreach(ele => {
//            println(ele)
//        })
//
//        import org.apache.spark.sql.types._
//        val b = ByteType
//        println(b)
//
//        def a(int: Int, string: String):Unit={
//            int + string
//
//        }
//        val number = StdIn.readLine().toInt
//
//        if(number < 100){
//            println("to low")
//        }else if(number > 0){
//            println("to hight")
//        }else{
//            println("correct")
//        }
//
//+-
//        val f=(b:Int, c:Int)=> b+c
//
//        println(f(2, 3))


        val df = spark.read.format("json").load("E:\\Git\\spark\\data\\flight-data\\json\\2015-summary.json")
        df.printSchema()

        import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
        import org.apache.spark.sql.types.Metadata

        val structType = StructType(List(
            StructField("ORIGIN_COUNTRY_NAME", StringType, false),
            StructField("DEST_COUNTRY_NAME", StringType, true),
            StructField("count", LongType, false,
                Metadata.fromJson("{\"hello\":\"world\"}"))
        ))


        val json = spark.sparkContext.textFile("E:\\Git\\spark\\data\\flight-data\\json\\2015-summary.json")
        val df3 = spark.read.schema(structType).json(json)
        val df2 = spark.read.format("json").schema(structType).load("E:\\Git\\spark\\data\\flight-data\\json\\2015-summary.json")
        df2.printSchema()
        df2.toDF().select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count").show(21)
        df3.toDF().select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count").show(21)
        df3.printSchema()
        val a = df2.schema.equals(structType)
        val b = df3.schema.equals(structType)
        println(a)
        println(b)

        import org.apache.spark.sql.functions.{col, column}

        df.col("count").alias("number").explain(true)
    }
}
