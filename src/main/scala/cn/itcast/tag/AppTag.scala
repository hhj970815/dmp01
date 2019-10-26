package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成app标签
  */
object AppTag extends TagProcess{
  override def makeTag(args: Any*): mutable.HashMap[String,Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、取出row与app广播变量
    val row = args.head.asInstanceOf[Row]
    val bc = args.last.asInstanceOf[Broadcast[Map[String, String]]]
    //2、从row中取出appid,appname
    val appId = row.getAs[String]("appid")
    var appName:String = row.getAs[String]("appname")
    //3、判断row中appname是否为空，如果为空，就从广播变量中根据appid取出appname
    if(!StringUtils.isNotBlank(appName)){
        appName = bc.value.getOrElse(appId,"")
    }
    //4、生成标签
    result.put(s"APP_${appName}",1)
    //5、标签的返回
    result
  }
}
