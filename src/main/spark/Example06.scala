import org.apache.spark.sql.SparkSession

/**
  * @Author: caoguoqing
  * @Date: @2021/2/9 16:40
  *        scala
  *        Content:
  */
object Example06 {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().master("local").appName("Exapmle06").getOrCreate()
        val df = sparkSession.read.format("csv").option("header",true).option("inferSchema","true").load("E:\\Git\\spark\\data\\retail-data\\by-day\\2010-12-01.csv")
        df.printSchema()
        df.createOrReplaceTempView("sq")
        import org.apache.spark.sql.functions.{column, col}
        val frame = sparkSession.read.format("json").load("E:\\git\\spark\\data\\flight-data\\json\\2015-summary.json")
        println(frame.schema)
        println(frame.col("count"))
        import org.apache.spark.sql.functions.lit
        println(df.select(lit(5), lit("five"), lit(5.0)))


        val col1 = col("col")
        println(col1.explain(true))
//        print($"my")
        println('mycolum)
        val column1 = column("column")
        println(column1)
        import org.apache.spark.sql.functions.expr
        println(expr("somecol -1"))
    }
}
