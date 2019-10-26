package cn.itcast.pro

import cn.itcast.`trait`.Process
import cn.itcast.utils.{ConfigUtils, DateUtils, IPAddressUtils, KuduUtils}
import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ParseIp extends Process{
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    *
    * @param spark
    * @param kuduContext
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取数据
    val source: DataFrame = spark.read.json(ConfigUtils.DATA_PATH)
    //指定数据存入的表名称
    val TABLE_NAME = s"ODS_${DateUtils.getNow()}"
    //2、处理数据
    //2.1、将ip提取出来
    import spark.implicits._
    //parquet、带有头信息的csv、json
    val ips = source.selectExpr("ip").as[String]
    //ip  -> ip  金纬度  省份 城市
    //2.2、根据ip解析经纬度、省份、城市
    val ipinfo: Dataset[(String, Float, Float, String, String)] = ips.map(ip => {
      val lookupService = new LookupService(ConfigUtils.GEOLITECITY_DAT)

      val location: Location = lookupService.getLocation(ip)
      //经度
      val longitude: Float = location.longitude
      //纬度
      val latitude: Float = location.latitude

      val iPAddressUtils = new IPAddressUtils

      val ipregion = iPAddressUtils.getregion(ip)
      //省份
      val region = ipregion.getRegion
      //城市
      val city = ipregion.getCity
      (ip, longitude, latitude, region, city)
    })
    //将ip解析信息注册成临时表
    ipinfo.toDF("ip","longitude","latitude","region","city").createOrReplaceTempView("ip_info")
    //将元数据也注册为临时表
    source.createOrReplaceTempView("t_data")
    //2.3、将解析出来的省份、城市、经纬度补充到元数据中
    val result: DataFrame = spark.sql(
      """
        select
         a.ip,
        a.sessionid,
        a.advertisersid,
        a.adorderid,
        a.adcreativeid,
        a.adplatformproviderid,
        a.sdkversion,
        a.adplatformkey,
        a.putinmodeltype,
        a.requestmode,
        a.adprice,
        a.adppprice,
        a.requestdate,
        a.appid,
        a.appname,
        a.uuid,
        a.device,
        a.client,
        a.osversion,
        a.density,
        a.pw,
        a.ph,
        b.longitude,
        b.latitude,
        b.region  as provincename,
        b.city as cityname,
        a.ispid,
        a.ispname,
        a.networkmannerid,
        a.networkmannername,
        a.iseffective,
        a.isbilling,
        a.adspacetype,
        a.adspacetypename,
        a.devicetype,
        a.processnode,
        a.apptype,
        a.district,
        a.paymode,
        a.isbid,
        a.bidprice,
        a.winprice,
        a.iswin,
        a.cur,
        a.rate,
        a.cnywinprice,
        a.imei,
        a.mac,
        a.idfa,
        a.openudid,
        a.androidid,
        a.rtbprovince,
        a.rtbcity,
        a.rtbdistrict,
        a.rtbstreet,
        a.storeurl,
        a.realip,
        a.isqualityapp,
        a.bidfloor,
        a.aw,
        a.ah,
        a.imeimd5,
        a.macmd5,
        a.idfamd5,
        a.openudidmd5,
        a.androididmd5,
        a.imeisha1,
        a.macsha1,
        a.idfasha1,
        a.openudidsha1,
        a.androididsha1,
        a.uuidunknow,
        a.userid,
        a.iptype,
        a.initbidprice,
        a.adpayment,
        a.agentrate,
        a.lomarkrate,
        a.adxrate,
        a.title,
        a.keywords,
        a.tagid,
        a.callbackdate,
        a.channelid,
        a.mediatype,
        a.email,
        a.tel,
        a.sex,
        a.age
         from t_data a left join ip_info b
         on a.ip = b.ip
      """.stripMargin)
    //3、写入kudu
    //指定kudu表的shema信息
    val schema = result.schema
    //指定表的主键字段
    val keys = Seq[String]("ip")
    //指定表的属性 表的分区数 分区规则 副本数
    val columns = Seq[String]("ip")
    val options = new CreateTableOptions

    import scala.collection.JavaConverters._
    options.addHashPartitions(columns.asJava,3)
    options.setNumReplicas(3)

    KuduUtils.write2Kudu(kuduContext,TABLE_NAME,result,schema,keys,options)
  }
}
