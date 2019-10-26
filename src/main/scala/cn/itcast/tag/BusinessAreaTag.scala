package cn.itcast.tag

import java.sql.{Connection, PreparedStatement}

import ch.hsr.geohash.GeoHash
import cn.itcast.`trait`.TagProcess
import cn.itcast.utils.JdbcUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 生成商圈标签
  */
object BusinessAreaTag extends TagProcess{
  /**
    * 生成对应的标签，具体逻辑由子类实现
    *
    * @param args
    */
  /*override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、获取row
    val row = args.head.asInstanceOf[Row]
    val statement = args.last.asInstanceOf[PreparedStatement]
    //2、取出经纬度
    //经度
    val longitude = row.getAs[Float]("longitude")
    //纬度
    val latitude = row.getAs[Float]("latitude")
    //3、将经纬度转成geoHash编码
    val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
    if(longitude!=null && latitude!=null){
      //4、根据geoHash编码从kudu表中查询商圈列表
      //商圈,商圈,商圈
      //val areas: String = JdbcUtils.query(geoHashCode)
      statement.setString(1,geoHashCode)
      val resultSet = statement.executeQuery()
      while (resultSet.next()){
        resultSet.getString("areas").split(",").foreach(x=>result.put(s"BA_${x}",1))
      }
      //6、数据返回
    }
    result
  }*/

  override def makeTag(args: Any*): mutable.HashMap[String, Double] = {
    val result = new mutable.HashMap[String,Double]()
    //1、获取row
    val row = args.head.asInstanceOf[Row]
    //2、取出经纬度
    val areas = row.getAs[String]("areas")

    if(StringUtils.isNoneBlank(areas))
      areas.split(",").foreach(x=>result.put(s"BA_${x}",1))
    //6、数据返回
    result
  }
}
