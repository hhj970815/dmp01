import cn.itcast.pro._
import cn.itcast.utils.ConfigUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

object Application {

  def main(args: Array[String]): Unit = {
    // 运行主类入口
    //1、创建SparkSession
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("app")
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()
    //2、创建kuducontext
    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    //3、数据处理
    //TODO 解析ip生成经纬度、省份、城市
    ParseIp.process(spark,kuduContext)
    //TODO 统计省市地域分布情况
    ProviceCityAnalysis.process(spark,kuduContext)
    //TODO 统计广告投放的地域分布情况
    AdRegionAnalysis.process(spark,kuduContext)
    //TODO 统计广告投放的APP分布情况
    AppAnaylysis.process(spark,kuduContext)
    //TODO 统计广告投放的手机设备类型分布情况
    AdDeviceTypeAnalysis.process(spark,kuduContext)
    //TODO 统计广告投放的网络类型分布情况
    AdNetworkAnalysis.process(spark,kuduContext)
    //TODO 统计广告投放的运行商分布情况
    AdOperatorAnalysis.process(spark,kuduContext)
    //TODO 统计广告投放的渠道分布情况
    AdChannelAnalysis.process(spark,kuduContext)
    //TODO 生成商圈库
    BusinessAreaPro.process(spark,kuduContext)
    //4、关闭资源
    spark.stop()
  }
}
