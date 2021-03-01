/**
  * @Author: caoguoqing
  * @Date: @2021-03-01 14:44
  *        scala
  *        Content:
  */
object Scala01 {
    class Person(val name:String, val age:Int){
        val this.name = name
        val this.age = age
    }
    def main(args: Array[String]): Unit = {
        val person = new Person("zhangsan", 10)
        println(person.name)
        println(BigInt(2).pow(1024))
        println(Math.pow(2,1024))
    }
}
