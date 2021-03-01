/**
  * @Author: caoguoqing
  * @Date: @2020/10/26 10:06
  *        scala
  *        Content:
  */
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext};

object WorldCount {


    def main(args: Array[String]): Unit = {
        //配置SparkConf名称为WorldCount
        val conf = new SparkConf().setAppName("WorldCount").setMaster("local")
        //创建SparkContext对象
        val sparkContext = new SparkContext(conf)
        //读取文件
        val file = sparkContext.textFile("C:\\Users\\hp\\Desktop\\localhost_access_log.txt")

        //解析文件,解析出访问的网页
        var rdd = file.map(line => {
            // 获取两个"之间的内容GET /MyDemoWeb/ HTTP/1.1
            var index1 = line.indexOf("\"")
            var index2 = line.lastIndexOf("\"")
            var access = line.substring(index1 + 1, index2)
            //获取两个空格之间的内容/MyDemoWeb/
            var index3 = access.indexOf(" ")
            var index4 = access.lastIndexOf(" ")
            var jsp = access.substring(index3 + 1, index4)
            //获取jsp
            var str = jsp.substring(jsp.indexOf("/") + 1)
            (str, 1)
        })
        //聚合
        var result = rdd.reduceByKey((a, b) => {
            a + b
        })
        //排序
        val key = result.sortBy(_._2, false)

        val finallyResult = key.map(ele => {
            (ele._1 +"," +ele._2).toString.replace("\\(", "").replace("\\)", "")

        }).collect()
        finallyResult.foreach(ele=>{
            println(ele)
        })

        //保存结果为csv文件
        def saveAsCsv(f: java.io.File)(op: java.io.PrintWriter => Unit){
            val printWriter = new PrintWriter(f)
            printWriter.write("word,")
            printWriter.write("num \n")
            try{
                op(printWriter)
            }finally{
                printWriter.close()
            }
        }
        //保存为csv文件
        saveAsCsv(new File("C:\\Users\\hp\\Desktop\\result.csv")){
            p=>finallyResult.foreach(p.println)
        }
        //保存文件
        key.saveAsTextFile("C:\\Users\\hp\\Desktop\\result.txt")
        sparkContext.stop()
    }

}
