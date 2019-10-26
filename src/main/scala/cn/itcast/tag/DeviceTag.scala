package cn.itcast.tag

import cn.itcast.`trait`.TagProcess
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成设备标签
  */
object DeviceTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {

    val result = new mutable.HashMap[String,Double]()
    //1、取出row与设备的广播变量
    val row = args.head.asInstanceOf[Row]
    val bc = args.last.asInstanceOf[Broadcast[Map[String, String]]]
    //2、取出设备[设备型号[DC]、设备类型[DT]、营运商[ISP]、联网方式[NT]、devicetype[DET]]
    //设备型号
    val device = row.getAs[String]("device")
    //设备类型 1-android ios wp
    val client = row.getAs[Long]("client")
    //运营商 移动 联通 电信
    val ispName = row.getAs[String]("ispname")
    //联网方式 WIFI 4G 3G 2G
    val networkMannerName = row.getAs[String]("networkmannername")
    //设备类型02
    val deviceType = row.getAs[Long]("devicetype")
    //3、将取出的值转换为企业内部编码
    //将设备类型转换为内部编码
    val clientCode = bc.value.getOrElse(client.toString,"")
    //将运营商名称转换为内部编码
    val ispCode = bc.value.getOrElse(ispName,"")
    //联网方式转换为内部编码
    val networkCode = bc.value.getOrElse(networkMannerName,"")
    //4、生成标签
    result.put(s"DC_${device}",1)
    result.put(s"DT_${clientCode}",1)
    result.put(s"ISP_${ispCode}",1)
    result.put(s"NT_${networkCode}",1)
    result.put(s"DET_${deviceType}".toString,1)
    //5、标签返回
    result

  }
}
