
import javassist.runtime.Desc
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: caoguoqing
  * @Date: @2021/1/3 20:48
  *        scala
  *        Content:
  */
object Example01 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("Example01")
            .master("spark://master:7077")
            .getOrCreate()

//        val myRange = spark.range(100).toDF("number")
//        val value = myRange.where("number % 2 = 0")
//        println(value.count())
        val fightData2015 = spark
          .read
          .option("inferSchema","true")
          .option("header","true")
          .csv("C:\\Users\\hp\\Desktop\\2015-summary.csv")
        println(fightData2015)
        spark.conf.set("spark.sql.shuffle.partitions", "3")
        println(fightData2015.sort("count").take(1).apply(0))

        fightData2015.createOrReplaceTempView("fight_data_2015")

        val sql = spark.sql(
            """
              select dest_country_name, count(*) from fight_data_2015 group by dest_country_name
            """)

        val df_select = fightData2015.groupBy("dest_country_name").count()

        val sql_max = spark.sql("select max(count) from fight_data_2015")
        sql_max.show()
        import org.apache.spark.sql.functions.max
        val max_count = fightData2015.select(max("count")).take(1)
        println(max_count.take(1).apply(0))


        val top5 = spark sql("select dest_country_name, sum(count) as destination_total from fight_data_2015 " +
          "group by dest_country_name " +
          "order by destination_total desc limit 5 ")
        top5.show()
        import org.apache.spark.sql.functions.desc
        val sumTop5 = fightData2015
          .groupBy("dest_country_name")
          .sum("count")
          .withColumnRenamed("sum(count)", "destination_total")
          .sort(desc("destination_total"))
          .limit(5).show()
        sql.show(100)
        df_select.show(100)
        spark.stop()

    }
}
