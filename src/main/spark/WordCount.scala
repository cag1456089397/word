import org.apache.spark.sql.SparkSession

/**
  * @ Author: caoguoqing
  * @ Date: @2021-02-23 16:04
  *        scala
  *        Content:
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        //创建会话
        val sparkSession = SparkSession.builder().master("local").appName("wc").getOrCreate()
        //读取文件
        val sc = sparkSession.sparkContext.textFile("datas")

        //扁平化
        val value = sc.flatMap(_.split(" "))

        val wordToOne = value.map(ele =>
            (ele, 1))

        wordToOne.reduceByKey(_+_).collect().foreach(print)
        val wordGroup = wordToOne.groupBy(ele => ele._1
        )

        val result = wordGroup.map {
            case (word, list) => {
                list.reduce((t1,t2)=>{
                    (t1._1,t1._2+t2._2)
                })
            }
        }

/*        //分组
        val valueGroup = value.groupBy(word=>word)

        //修改结构
        val result = valueGroup.map {
            case (word, list) => {
                (word, list.size)

            }
        }*/

        val array = result.collect()
        array.foreach(ele=>{
            println(ele._1, ele._2)
        })

        //关闭会话
        sparkSession.stop()

    }
}
