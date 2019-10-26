package cn.itcast.pro

import cn.itcast.`trait`.Process
import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的手机设备类型分布情况
  */
object AdDeviceTypeAnalysis extends Process{
  //指明数据读取的表名
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //指明数据写入的表名
  val SINK_TABLE = s"ad_device_type_analysis_${DateUtils.getNow()}"
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
      .option("kudu.table",SOURCE_TABLE)
      .kudu
      .createOrReplaceTempView("ods")
    //2、统计
    spark.sql(
      """
        |select
        |   case when client=1 then 'IOS' when client=2 then 'android'
        |   when client=3 then 'wp' else 'other' end as client,device,
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
        |  group by client,device
      """.stripMargin).createOrReplaceTempView("tmp")
    //2.2、根据2.1计算出竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | client,device,
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


    //3、写入kudu
    //指定表的schema信息
    val schema = result.schema
    //指定表的主键字段
    val keys = Seq[String]("client","device")
    val options = new CreateTableOptions
    //指定表的分区策略 分区个数 分区字段
    import scala.collection.JavaConverters._
    options.addHashPartitions(keys.asJava,3)
    //指定副本数
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext,SINK_TABLE,result,schema,keys,options)
  }
}
