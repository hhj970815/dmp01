package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 关键字标签生成
  */
object KeywordTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、取出row对象
    val row = args.head.asInstanceOf[Row]
    //2、从row中取出关键字
    val keywords: String = row.getAs[String]("keywords")
    //3、对关键字进行切割，每一个关键字都是一个标签
    keywords.split(",").foreach(x=>result.put(s"KW_${x}",1))
    //4、数据返回
    result
  }
}
