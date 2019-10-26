package cn.itcast.utils

import com.typesafe.config.ConfigFactory

object ConfigUtils {

  //加载配置文件
  val conf = ConfigFactory.load()

  //sparksql自动广播小表大小限制
  val SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD = conf.getString("spark.sql.autoBroadcastJoinThreshold")
  //sparksql shuffle分区数
  val SPARK_SQL_SHUFFLE_PARTITIONS=conf.getString("spark.sql.shuffle.partitions")
  //spark shuffle重试次数
  val SPARK_SHUFFLE_IO_MAXRETRIES=conf.getString("spark.shuffle.io.maxRetries")
  //sparksql shuffle重试间隔
  val SPARK_SHUFFLE_IO_RETRYWAIT=conf.getString("spark.shuffle.io.retryWait")
  //spark序列化
  val SPARK_SERIALIZER=conf.getString("spark.serializer")
  //spark执行与存储内存占比
  val SPARK_MEMORY_FRACTION=conf.getString("spark.memory.fraction")
  //spark存储内存占比
  val SPARK_MEMORY_STORAGEFRACTION=conf.getString("spark.memory.storageFraction")
  //sparkcore shuffle分区数
  val SPARK_DEFAULT_PARALLELISM=conf.getString("spark.default.parallelism")
  //是否启动推测机制
  val SPARK_SPECULATION=conf.getString("spark.speculation.flag")
  //推测机制启动时机
  val SPARK_SPECULATION_MULTIPLIER=conf.getString("spark.speculation.multiplier")
  //kudu master地址
  val KUDU_MASTER=conf.getString("kudu.master")
  //数据路径
  val DATA_PATH = conf.getString("data.path")
  //纯真数据库文件名称
  val IP_FILE = conf.getString("IP_FILE")
  //纯真数据库所在路径
  val INSTALL_DIR = conf.getString("INSTALL_DIR")

  val GEOLITECITY_DAT = conf.getString("GeoLiteCity.dat")
  //商圈库获取接口地址
  val URL = conf.getString("URL")

  //app字典文件路径
  val APP_DIRECT_PATH = conf.getString("app.dirct.path")
  //设备字典文件路径
  val DEVICE_DIRECT_PATH = conf.getString("device.direct.path")
}
