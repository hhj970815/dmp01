package cn.itcast.pro

import cn.itcast.`trait`.Process
import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.ivy.util.DateUtil
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的地域分布情况
  */
object AdRegionAnalysis extends Process{

  //指明数据读取表
  val SOUCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //指明数据存入表名
  val SINK_TABLE = s"ad_region_analysis_${DateUtils.getNow()}"
  /**
    * 创建抽象方法，具体实现逻辑由子类实现
    *
    * @param spark
    * @param kuduContext
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取ODS数据
    import org.apache.kudu.spark.kudu._
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",SOUCE_TABLE)
      .kudu
      .createOrReplaceTempView("ods")
    //2、数据处理
    //2.1、统计原始请求数、有效请求数、广告请求数、参数竞价数、竞价成功数、展示量、点击量、广告成本、广告消费
    spark.sql(
      """
        |select
        |   provincename,cityname,
        |   sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |   sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) valid_request_num,
        |   sum(case when requestmode=1 and processnode=3 then 1 else 0 end) ad_request_num,
        |   sum(case when adplatformproviderid>=100000 and iseffective=1
        |       and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_num,
        |    sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_num,
        |   sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_person_show_num,
        |   sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_person_click_num,
        |   sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |   sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |   sum(case when adplatformproviderid>=100000
        |     and iseffective=1 and isbilling=1 and iswin=1
        |     and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) dsp_consumtion,
        |   sum(case when adplatformproviderid>=100000
        |     and iseffective=1 and isbilling=1 and iswin=1
        |     and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) dsp_cost
        |  from ods
        |  group by provincename,cityname
      """.stripMargin).createOrReplaceTempView("tmp")
    //2.2、根据2.1计算出竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | provincename,cityname,
        | org_request_num,
        | valid_request_num,
        | ad_request_num,
        | bid_num,
        | bid_success_num,
        | bid_success_num/bid_num as bid_success_rat,
        | ad_person_show_num,
        | ad_person_click_num,
        | media_show_num,
        | media_click_num,
        | media_click_num / media_show_num media_click_rat,
        | dsp_consumtion,
        | dsp_cost
        | from tmp
      """.stripMargin)

    //3、数据写入kudu
    val schema = result.schema

    //表的主键字段
    val keys = Seq[String]("provincename","cityname")
    //表的属性
    val options = new CreateTableOptions
    import scala.collection.JavaConverters._
    //设置表的分区策略 分区数
    options.addHashPartitions(keys.asJava,3)
    //设置副本数
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,SINK_TABLE,result,schema,keys,options)

  }

}
