package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成地域标签
  */
object RegionTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、从args中取出row
    val row = args.head.asInstanceOf[Row]
    //2、从row中获取省、市
    //省
    val proviceName = row.getAs[String]("provincename")
    //市
    val cityName = row.getAs[String]("cityname")
    //3、生成标签
    result.put(s"RN_${proviceName}",1)
    result.put(s"CT_${cityName}",1)
    //4、数据返回
    result
  }
}
