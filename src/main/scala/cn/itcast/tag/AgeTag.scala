package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成年龄标签
  */
object AgeTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、获取row对象
    val row = args.head.asInstanceOf[Row]
    //2、获取age字段值
    val age = row.getAs[String]("age")
    //3、生成标签
    result.put(s"AGE_${age}",1)
    //4、数据返回
    result
  }
}
