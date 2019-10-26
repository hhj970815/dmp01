package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成渠道标签
  */
object ChannelTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、读取row对象
    val row = args.head.asInstanceOf[Row]
    //2、从row对象获取渠道字段的值
    val channel = row.getAs[String]("channelid")
    //3、生成标签
    result.put(s"CN_${channel}",1)
    //4、数据返回
    result
  }
}
