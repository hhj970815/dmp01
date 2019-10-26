package cn.itcast.pro

import java.sql.{Connection, DriverManager, PreparedStatement}

import ch.hsr.geohash.GeoHash
import cn.itcast.`trait`.Process
import cn.itcast.agg.TagAgg
import cn.itcast.graph.UserGraph
import cn.itcast.history.HistoryTags
import cn.itcast.tag._
import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TagPro extends Process{
  //定义数据读取表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //定义商圈表
  val BUSINESS_TABLE = s"businessArea_${DateUtils.getNow()}"
  //定义历史表的表名 yyyyMMdd
  val HISTORY_TABLE = s"tags_${DateUtils.getYesterDay()}"
  //定义标签数据存入表
  val SINK_TABLE = s"tags_${DateUtils.getNow()}"
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    * 标签:
    *   app【APP】、设备[设备型号[DC]、设备类型[DT]、营运商[ISP]、联网方式[NT]、devicetype[DET]]、地域[省份[RN]、城市[CT]]、关键字[KW]、渠道[CN]、年龄[AGE]、性别[SEX]、商圈库[BA]
    *
    *
    *   用户标识
    *
    * 当前问题:
    *   目前我们只对当天的数据进行处理，我们还需要将历史数据读取出来，与当天数据一起做标签合并处理
    *   1、历史数据中可能存在与当天数据相同的用户，这样的就会导致同一个用户出现多条记录的情况
    *      解决方案:用统一用户识别
    *   2、历史数据与当天数据中存在相同用户的时候，标签权重如何处理
    *      解决方案:需要将历史数据的标签权重进行衰减，衰减之后在与当天权重累加
    *
    * @param spark
    * @param kuduContext
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取ODS表数据
    import org.apache.kudu.spark.kudu._
    val source: DataFrame = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",SOURCE_TABLE)
      .kudu

    //读取商圈表
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",BUSINESS_TABLE)
      .kudu
      .createOrReplaceTempView("business_area")
    //2、数据处理
    //2.1、过滤非法数据
    val filterSource: Dataset[Row] = source.filter(
      """
        |(imei is not null and imei!='') or
        |(mac is not null and mac!='') or
        |(idfa is not null and idfa!='') or
        |(openudid is not null and openudid!='') or
        |(androidid is not null and androidid!='')
      """.stripMargin)
    //2.2、去重[/]
    //2.3、将字典文件读取出来
    //企业中有可能存在name之类的字段为空的情况，这时候需要根据字典文件获取name值
    //将数据中的个别字段的枚举值转成企业内部的编码
    //因为字典文件后期所有的task都需要，那么为了减少宽带资源与传输时间以及内存的占用，所有将这份公共数据广播出去
    //2.3.1、读取app字典文件
    val appDirct: Dataset[String] = spark.read.textFile(ConfigUtils.APP_DIRECT_PATH)
    import spark.implicits._
    val appData = appDirct.map(line=>{
      val arr = line.split("##")
      val appId = arr.head
      val appName = arr.last
      (appId,appName)
    }).collect().toMap

    val appBc: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(appData)
    //2.3.2、读取设备字典文件，并广播出去
    val deviceDirect = spark.read.textFile(ConfigUtils.DEVICE_DIRECT_PATH)
    val deviceData = deviceDirect.map(line=>{
      val arr = line.split("##")
      val arrValue = arr.head
      val arrCode = arr.last
      (arrValue,arrCode)
    }).collect().toMap

    val deviceBc: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(deviceData)


    //将filterSource与商圈表进行join，join的条件：编码(经度，纬度) = geoHash
    filterSource.createOrReplaceTempView("source")
    //注册udf函数
    spark.udf.register("geoHashCode",geoHashCode _)
    spark.sql("cache table business_area")

    val newSource= spark.sql(
      """
        |select s.*,b.areas
        | from source s left join business_area b
        | on geoHashCode(s.longitude,s.latitude) = b.geoHash
      """.stripMargin)
    newSource.explain()
    val userData: RDD[(String, (ArrayBuffer[String], mutable.Map[String, Double]))] = newSource.rdd.map(row=>{
      //1、app标签
      val appTags = AppTag.makeTag(row,appBc)
      //2、设备标签
      val deviceTags = DeviceTag.makeTag(row,deviceBc)
      //3、地域标签
      val regionTags = RegionTag.makeTag(row)
      //4、关键字标签
      val keywordsTags = KeywordTag.makeTag(row)
      //5、渠道标签
      val channelTags = ChannelTag.makeTag(row)
      //6、年龄标签
      val ageTags = AgeTag.makeTag(row)
      //7、性别标签
      val sexTags = SexTag.makeTag(row)
      //8、商圈标签
      val areaTags = BusinessAreaTag.makeTag(row)
      //所有标签
      val allTags: mutable.Map[String, Double] = appTags ++ deviceTags ++ regionTags++sexTags++keywordsTags++channelTags++ageTags++sexTags++areaTags
      //用户标识
      val allUserIds: ArrayBuffer[String] = getAllUserIds(row)
      //暂时用用户所有标识中第一个不为空的标识表示为一个用户
      val userId = allUserIds.head
      (userId,(allUserIds,allTags))
    })

    //用户统一识别
    val graphData: RDD[(VertexId, (ArrayBuffer[String], mutable.Map[String, Double]))] = UserGraph.graph(userData)
    //标签聚合
    val result: RDD[(String, (mutable.Buffer[String], Map[String, Double]))] = TagAgg.agg(graphData)

    val allRDD: RDD[(String, (mutable.Buffer[String], Map[String, Double]))] = HistoryTags.process(spark,result)

    val resultDF = allRDD.map(element =>{
      element match {
        case (userid,(allUserIds,tags))=>
          val allUserIdsStr = allUserIds.mkString(",")
          val tagsStr = tags.mkString(",")
          (userid,allUserIdsStr,tagsStr)
      }
    }).toDF("userid","allUserId","tags")
    //定义shema
    val schema = resultDF.schema
    //定义表的主键
    val keys = Seq[String]("userid")
    //定义表的属性
    val options = new CreateTableOptions
    //定义表的分区策略 分区字段 分区数
    import scala.collection.JavaConverters._
    options.addHashPartitions(keys.asJava,3)
    //设置副本数
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,SINK_TABLE,resultDF,schema,keys,options)

    //生成历史数据
    /*val resultDF = result.map(element =>{
      element match {
        case (userid,(allUserIds,tags))=>
          val allUserIdsStr = allUserIds.mkString(",")
          val tagsStr = tags.mkString(",")
          (userid,allUserIdsStr,tagsStr)
      }
    }).toDF("userid","allUserId","tags")
    //定义shema
    val schema = resultDF.schema
    //定义表的主键
    val keys = Seq[String]("userid")
    //定义表的属性
    val options = new CreateTableOptions
    //定义表的分区策略 分区字段 分区数
    import scala.collection.JavaConverters._
    options.addHashPartitions(keys.asJava,3)
    //设置副本数
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,HISTORY_TABLE,resultDF,schema,keys,options)*/
    //2.4、生成标签
    /*filterSource.rdd.map(row=>{
      //1、app标签
      val appTags = AppTag.makeTag(row,appBc)
      //2、设备标签
      val deviceTags = DeviceTag.makeTag(row,deviceBc)
      //3、地域标签
      val regionTags = RegionTag.makeTag(row)
      //4、关键字标签
      val keywordsTags = KeywordTag.makeTag(row)
      //5、渠道标签
      val channelTags = ChannelTag.makeTag(row)
      //6、年龄标签
      val ageTags = AgeTag.makeTag(row)
      //7、性别标签
      val sexTags = SexTag.makeTag(row)
      //8、商圈标签
      val areaTags = BusinessAreaTag.makeTag(row)
      appTags ++ deviceTags ++ regionTags++sexTags++keywordsTags++channelTags++ageTags++sexTags++areaTags
    }).foreach(println(_))*/

    /*filterSource.rdd.mapPartitions(it=>{
      var connection:Connection = null
      var statement:PreparedStatement = null

      try{
        //1、加载驱动
        Class.forName("com.cloudera.impala.jdbc41.Driver")
        //2、获取connection
        connection = DriverManager.getConnection("jdbc:impala://hadoop01:21050/default")
        //3、创建statement
        statement = connection.prepareStatement(s"select areas from businessArea_${DateUtils.getNow()} where geoHash=?")
        while (it.hasNext){
          val row = it.next()
          //1、app标签
          val appTags = AppTag.makeTag(row,appBc)
          //2、设备标签
          val deviceTags = DeviceTag.makeTag(row,deviceBc)
          //3、地域标签
          val regionTags = RegionTag.makeTag(row)
          //4、关键字标签
          val keywordsTags = KeywordTag.makeTag(row)
          //5、渠道标签
          val channelTags = ChannelTag.makeTag(row)
          //6、年龄标签
          val ageTags = AgeTag.makeTag(row)
          //7、性别标签
          val sexTags = SexTag.makeTag(row)
          //8、商圈标签
          val areaTags = BusinessAreaTag.makeTag(row,statement)
          appTags ++ deviceTags ++ regionTags++sexTags++keywordsTags++channelTags++ageTags++sexTags++areaTags
        }
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }

      it
    }).foreach(println(_))*/
    //........

  }

  /**
    * 自定义UDF函数，将经纬度变成geoHash编码
    * @param longitude
    * @param latitude
    * @return
    */
  def geoHashCode(longitude:Float,latitude:Float):String={
    GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
  }

  /**
    * 获取用户所有标识
    * @param row
    */
  def getAllUserIds(row:Row)={

    val result = new ArrayBuffer[String]()
    val imei = row.getAs[String]("imei")
    val mac = row.getAs[String]("mac")
    val idfa = row.getAs[String]("idfa")
    val openudid = row.getAs[String]("openudid")
    val androidid = row.getAs[String]("androidid")

    if(StringUtils.isNoneBlank(imei)){
      result.append(imei)
    }
    if(StringUtils.isNoneBlank(mac)){
      result.append(mac)
    }
    if(StringUtils.isNoneBlank(idfa)){
      result.append(idfa)
    }
    if(StringUtils.isNoneBlank(openudid)){
      result.append(openudid)
    }
    if(StringUtils.isNoneBlank(androidid)){
      result.append(androidid)
    }

    result
  }
}
