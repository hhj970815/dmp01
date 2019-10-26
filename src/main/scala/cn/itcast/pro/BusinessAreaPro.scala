package cn.itcast.pro

import java.util

import ch.hsr.geohash.GeoHash
import cn.itcast.`trait`.Process
import cn.itcast.utils.{ConfigUtils, DateUtils, HttpUtils, KuduUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * 生成商圈库信息
  */
object BusinessAreaPro extends Process{
  //指定数据读取的表
  val SOUCE_TABLE = s"ODS_${DateUtils.getNow()}"

  //定义数据保存的表名
  val SINK_TABLE = s"businessArea_${DateUtils.getNow()}"
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    *
    * @param spark
    * @param kuduContext
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {

    //1、读取ODS数据
    import org.apache.kudu.spark.kudu._
    import spark.implicits._
    val source = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",SOUCE_TABLE)
      .kudu
    //2、获取经纬度
    val regionData = source.selectExpr("longitude","latitude")
    //3、过滤经纬度为空的数据
    val filterData = regionData.filter("longitude is not null and latitude is not null")
    //4、去重
    val distinctData = filterData.distinct()
    //5、通过http请求获取商圈列表 （经度，纬度) => (经度，纬度，商圈信息)
    val ds = distinctData.as[(Float,Float)]
    val result = ds.map(x=>{
      //经度
      val longitude = x._1
      //纬度
      val latitude = x._2
      //商圈库请求url
      val url = ConfigUtils.URL.format(s"${longitude},${latitude}")
      //发起http请求
      val responseStr = HttpUtils.get(url)
      //6、解析json数据
      val areas: String = parseJson(responseStr)
      //7、对经纬度进行geohash编码 将一块区域的经纬度通过geohash编码之后，编程同一个code
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)

      (geoHash,areas)
    }).toDF("geoHash","areas")
      .filter("areas!=''")
      .distinct()

    //8、将数据保存到kudu
    //定义shema
    val schema = result.schema
    //定义主键字段
    val keys = Seq("geoHash")
    //设置表的属性信息
    val options = new CreateTableOptions
    //设置表分区策略 分区字段 分区个数
    options.addHashPartitions(keys.asJava,3)
    //设置副本数
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,SINK_TABLE,result,schema,keys,options)
  }

  /**
    * 解析json
    * @param json
    */
  def parseJson(json:String)={
    try{

      //将json转转成jsnObject
      val obj: JSONObject = JSON.parseObject(json)

      val regeocodeObj: JSONObject = obj.getJSONObject("regeocode")

      val addressComponent = regeocodeObj.getJSONObject("addressComponent")

      val businessAreas = addressComponent.getJSONArray("businessAreas")

      val areases: util.List[BusinessArea] = businessAreas.toJavaList(classOf[BusinessArea])

      val areas: mutable.Buffer[BusinessArea] = areases.asScala
      //name,name,name
      areas.map(_.name).mkString(",")
    }catch {
      case e:Exception => ""
    }

  }
}

case class BusinessArea(location:String,name:String,id:String)
