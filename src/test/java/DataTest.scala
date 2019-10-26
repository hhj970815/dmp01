import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test

import scala.util.Random

/**
  * 数据倾斜出现情况
  * 1、join的时候如果key为空，那么这些为空的数据全部都会聚集在一个task中
  * 2、并行度设置过少， key%分区数=分区号
  * 3、groupBy的时候造成数据倾斜  group by city
  * 4、大表join小表
  * 5、大表join大表  其中一个大表存在个别key倾斜，另外一个大表分布比较均匀
  * 6、大表join大表  其中一个大表存在很多key倾斜，另外一个大表分布比较均匀
  */
class DataTest extends Serializable {

    val spark = SparkSession.builder().appName("test")
            .master("local[4]")
            .getOrCreate()

    import spark.implicits._

    /**
      * 1、join的时候如果key为空，那么这些为空的数据全部都会聚集在一个task中
      * * 解决:过滤掉这些key为空数据
      */
    @Test
    def joinNull(): Unit ={
        val student = Seq[(Int,String,Int,String)](
            (1,"张三1",20,""),
            (2,"张三2",20,""),
            (3,"张三3",20,""),
            (4,"张三4",20,""),
            (5,"张三5",20,""),
            (6,"张三6",20,""),
            (7,"张三7",20,""),
            (8,"张三8",20,"class_01"),
            (9,"张三9",20,"class_02"),
            (10,"张三10",20,"class_03")
        )

        val clazz = Seq[(String,String)](
            ("class_01","python就业班"),
            ("class_02","大数据就业班"),
            ("class_03","java就业班")
        )

        student.toDF("id", "name", "age", "clazz_id")
//                .filter("clazz_id is not null and clazz_id!=''")
                .createOrReplaceTempView("student")

        clazz.toDF("id", "name")
                .createOrReplaceTempView("clazz")

        def func(index: Int, it: Iterator[Row]): Iterator[Row] = {
            println(s"index:${index}   data:${it.toBuffer}")
            it
        }

        spark.sql(
            """
              |select s.id,s.name,c.name
              | from student s left join clazz c
              | on s.clazz_id = c.id
            """.stripMargin).rdd.mapPartitionsWithIndex(func).collect()
        Thread.sleep(10000000)
    }

    /**
      * 解决方案:
      *   采用局部聚合+全局聚合
      *   通过自定义函数加盐进行局部聚合,再进行全局聚合
      */
    @Test
    def solution2(): Unit = {
        val student = Seq[(Int, String, Int, String)](
            (1, "张三1", 20, "class_01"),
            (2, "张三2", 20, "class_01"),
            (3, "张三3", 20, "class_01"),
            (4, "张三4", 20, "class_01"),
            (5, "张三5", 20, "class_01"),
            (6, "张三6", 20, "class_01"),
            (7, "张三7", 20, "class_01"),
            (8, "张三8", 20, "class_01"),
            (9, "张三9", 20, "class_02"),
            (10, "张三10", 20, "class_03")
        )

        val clazz = Seq[(String, String)](
            ("class_01", "python就业班"),
            ("class_02", "大数据就业班"),
            ("class_03", "java就业班")
        )

        spark.udf.register("addPrifix", addPrifix _)
        spark.udf.register("unPrifix", unPrifix _)

        student.toDF("id", "name", "age", "clazz_id")
                .selectExpr("id", "name", "age", "addPrifix(clazz_id) clazz_id_new", "clazz_id")
                .createOrReplaceTempView("student")

        clazz.toDF("id", "name").createOrReplaceTempView("class")

        spark.sql(
            """
              |select tmp.clazz_id, sum(tmp.num) from (select clazz_id_new, clazz_id, count(1) num from student s group by clazz_id_new, clazz_id) tmp group by tmp.clazz_id
            """.stripMargin).show()

//        spark.sql(
//            """
//              |select unPrifix(tmp.clazz_id_new) class_id, sum(tmp.num) num from (select clazz_id_new, count(1) num from student s group by clazz_id_new) tmp
//            """.stripMargin).show()

    }

    /**
      * 加盐----为字段值加上一个随机数
      * @param str
      * @return
      */
    def addPrifix(str: String): String ={
        s"${Random.nextInt(10)}#${str}"
    }

    /**
      * 去掉前缀
      * @param str
      * @return
      */
    def unPrifix(str:String):String={
        str.split("#").last
    }

    /**
      * 大表join小表
      * 使用广播,将小表广播出去,要加spark.sql("cache table 表名")
      */
    @Test
    def solution3(): Unit = {
        val student = Seq[(Int, String, Int, String)](
            (1, "张三1", 20, "class_01"),
            (2, "张三2", 20, "class_01"),
            (3, "张三3", 20, "class_01"),
            (4, "张三4", 20, "class_01"),
            (5, "张三5", 20, "class_01"),
            (6, "张三6", 20, "class_01"),
            (7, "张三7", 20, "class_01"),
            (8, "张三8", 20, "class_01"),
            (9, "张三9", 20, "class_02"),
            (10, "张三10", 20, "class_03")
        )

        val clazz = Seq[(String, String)](
            ("class_01", "python就业班"),
            ("class_02", "大数据就业班"),
            ("class_03", "java就业班")
        )

        student.toDF("id", "name", "age", "clazz_id")
                .createOrReplaceTempView("student")
        clazz.toDF("id", "name")
                .createOrReplaceTempView("clazz")
        // 有时候工作中, 小表没有达到自动广播的参数的上限,但是也没有广播出去的时候
        spark.sql("cache table clazz")

        spark.sql(
            """
              |select s.id, s.name, c.name from student s left join clazz c on s.clazz_id=c.id
            """.stripMargin).show()
    }

    /**
      * 大表join大表  其中一个大表存在个别key倾斜，另外一个大表分布比较均匀
      * 解决方案:
      *   将产生数据倾斜的key过滤出来单独处理，其他没有产生倾斜的key照常处理
      */
    @Test
    def solution5(): Unit = {
        val student = Seq[(Int, String, Int, String)](
            (1, "张三1", 20, "class_01"),
            (2, "张三2", 20, "class_01"),
            (3, "张三3", 20, "class_01"),
            (4, "张三4", 20, "class_01"),
            (5, "张三5", 20, "class_01"),
            (6, "张三6", 20, "class_01"),
            (7, "张三7", 20, "class_01"),
            (8, "张三8", 20, "class_01"),
            (9, "张三9", 20, "class_02"),
            (10, "张三10", 20, "class_03")
        )

        val clazz = Seq[(String, String)](
            ("class_01", "python就业班"),
            ("class_02", "大数据就业班"),
            ("class_03", "java就业班")
        )
        //注册udf
        spark.udf.register("addPrifix", addPrifix _)
        spark.udf.register("prfiex", prfiex _)

        val studentDF = student.toDF("id","name","age","clazz_id")
        //产生倾斜的数据
        studentDF.filter("clazz_id='class_01'")
                .selectExpr("id","name","age","addPrifix(clazz_id) clazz_id")
                .createOrReplaceTempView("student01")
        //过滤没有产生倾斜数据
        studentDF.filter("clazz_id!='class_01'")
                .createOrReplaceTempView("student02")

        //过滤产生倾斜的key
        val clazzDF = clazz.toDF("id","name")
                .filter("id='class_01'")

        //对产生数据倾斜key进行扩容
        kuorong(clazzDF,spark).createOrReplaceTempView("clazz01")
        //没有产生数据倾斜的key
        clazz.toDF("id","name")
                .filter("id!='class_01'")
                .createOrReplaceTempView("clazz02")
        /**
          * 判断哪些key产生倾斜
          *  1、select key,count(1) from table group by key
          *
          *  2、采样
          *
          */
        //studentDF.selectExpr("clazz_id").sample(false,0.5).show()
        //没有产生倾斜的key照常处理
        spark.sql(
            """
              |select s.id,s.name,c.name
              | from student02 s left join clazz02 c
              | on s.clazz_id = c.id
            """.stripMargin).createOrReplaceTempView("tmp02")


        spark.sql(
            """
              |select s.id,s.name,c.name
              | from student01 s left join clazz01 c
              | on s.clazz_id = c.id
            """.stripMargin).createOrReplaceTempView("tmp01")


        spark.sql(
            """
              |select * from tmp01
              |union
              |select * from tmp02
            """.stripMargin).show

    }

    /**
      * 扩容表
      * @param clazzDF
      * @param spark
      * @return
      */
    def kuorong(clazzDF:DataFrame,spark:SparkSession) ={
        //创建空的dataFrame
        var result = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],clazzDF.schema)
        for(i<- 0 to 10){
            result = result.union(clazzDF.selectExpr(s"prfiex(${i},id) as id","name"))
        }
        result
    }

    /**
      * 添加前缀
      * @param i
      * @param str
      * @return
      */
    def prfiex(i:Int,str:String):String={
        s"${i}#${str}"
    }

    /**
      * 大表join大表  其中一个大表存在很多key倾斜，另外一个大表分布比较均匀
      * 解决:
      *   将产生数据倾斜的表对应key加上随机数
      *   将均匀的表直接扩容 [10以内]
      */
    @Test
    def solution6(): Unit ={
        val student = Seq[(Int,String,Int,String)](
            (1,"张三1",20,"class_01"),
            (2,"张三2",20,"class_01"),
            (3,"张三3",20,"class_01"),
            (4,"张三4",20,"class_01"),
            (5,"张三5",20,"class_01"),
            (6,"张三6",20,"class_01"),
            (7,"张三7",20,"class_01"),
            (8,"张三8",20,"class_01"),
            (9,"张三9",20,"class_02"),
            (10,"张三9",20,"class_02"),
            (11,"张三9",20,"class_02"),
            (12,"张三9",20,"class_02"),
            (13,"张三9",20,"class_02"),
            (14,"张三9",20,"class_02"),
            (15,"张三10",20,"class_03")
        )

        val clazz = Seq[(String,String)](
            ("class_01","python就业班"),
            ("class_02","大数据就业班"),
            ("class_03","java就业班")
        )
        //注册udf
        spark.udf.register("addPrifix",addPrifix _)
        spark.udf.register("prfiex",prfiex _)

        val studentDF = student.toDF("id","name","age","clazz_id")

        val clazzDF = clazz.toDF("id","name")


        studentDF.selectExpr("id","name","age","addPrifix(clazz_id) clazz_id")
                .createOrReplaceTempView("student")

        kuorong(clazzDF,spark).createOrReplaceTempView("clazz")

        // 测试
        spark.sql(
            """
              |select * from clazz
            """.stripMargin).show()

//        spark.sql(
//            """
//              |select s.id,s.name,c.name
//              | from student s left join clazz c
//              | on s.clazz_id = c.id
//            """.stripMargin).show
    }
}
