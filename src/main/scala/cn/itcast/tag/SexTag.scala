package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 性别标签生成
  */
object SexTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、获取row对象
    val row = args.head.asInstanceOf[Row]
    //2、获取sex字段值
    val sex = row.getAs[String]("sex")
    //3、生成标签
    result.put(s"SEX_${sex}",1)
    //4、数据返回
    result
  }
}
