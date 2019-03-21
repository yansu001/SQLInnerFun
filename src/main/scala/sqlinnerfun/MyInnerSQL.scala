package sqlinnerfun

import java.sql.{Connection, Date, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import utils.AESUtil

import scala.collection.mutable.ListBuffer

/**
  * @author yansu
  * @date 2019/03/20 10:44
  */
object MyInnerSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("inner sql test")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = SparkSession.builder().getOrCreate()

    val data = sc.textFile("E:\\test\\sqldata.txt")
    val scheam = StructType {
      Seq(
        StructField("mac", StringType, false),
        StructField("phone_brand", StringType, true),
        StructField("enter_time", TimestampType, true),
        StructField("first_time", TimestampType, true),
        StructField("last_time", TimestampType, true),
        StructField("region", StringType, true),
        StructField("screen", StringType, true),
        StructField("stay_long", IntegerType, true)
      )
    }
    val rowRDD: RDD[Row] = data.map(row => {
      val arr = row.split(",")
      val mac = arr(0);
      val phone_brand = arr(1)
      val enter_time = Str2Date(arr(2))
      val first_time = Str2Date(arr(3))
      val last_time = Str2Date(arr(4))
      val region = arr(5)
      val screen = arr(6)
      val stay_long = arr(7).toInt
      Row(mac, phone_brand, enter_time, first_time, last_time, region, screen, stay_long)
    })
    val dataDF = ssc.createDataFrame(rowRDD, scheam);

    dataDF.createOrReplaceTempView("info")

    ssc.udf.register("encryptUDF", encryptMac _)
    ssc.udf.register("decruptUDF", decryptMac _)
    // sql内置函数
    val sql =
      """
        |select
        |decruptUDF(encryptUDF(mac,'123456'),'123456') as dmac,
        |encryptUDF(mac,'123456') as id, mac,
        |abs(stay_long) as res_abs,
        |coalesce(mac,screen,stay_long) as res_coalesce,
        |greatest(enter_time,first_time,last_time) as res_greatest,
        |if(stay_long = 0,'false','true') as res_if,
        |if(isnan(mac),1,0) as res_isnan,
        |current_timestamp() as current_time
        |from info
      """.stripMargin
    val strSQL =
      """
        |select
        |ascii(mac) as res_ascii,
        |base64(mac) as res_base64,
        |unbase64(mac) as res_unbase64,
        |concat(mac,phone_brand) as res_concat,
        |encode(mac,'UTF-8') as res_encode,
        |decode(mac,'UTF-8') as res_decode,
        |format_number(stay_long,1) as res_format
        |from info
      """.stripMargin

    val strSQL1 =
      """
        |select
        |screen,
        |initcap(mac) as res_initcap,
        |lower(mac) as res_lower,upper(mac) as res_upper,
        |lpad(mac,20,'**') as left, rpad(mac,20,'$$') as right,
        |trim(mac) as res_trim
        |from info
        |where
        |screen rlike '国.*'
      """.stripMargin
    val strSQL2 =
      """
        |select mac,
        |'http://facebook.com/path/p1.php?query=1#Ref' as url,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','HOST') as host,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','PATH') as path,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','QUERY') as query,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','PROTOCOL') as protocol,
        |parse_url('http://facebook.com/path/p1.php?query=1#引用','REF') as ref
        |from info
      """.stripMargin

    val sql1 =
      """
        |select
        |mac,rank() over(partition by screen order by stay_long) as res_rank,
        |row_number() over(partition by screen order by stay_long) as res_row_number,
        |dense_rank() over(partition by screen order by stay_long) as res_des_rank
        |from info
      """.stripMargin
    val sql2 =
      """
        |select
        |substr(mac,0,3) as res_startend,
        |substr(phone_brand,4) res_start,
        |substring(mac,2,4) res_2_4,
        |translate(mac,'0','&') as res_replace,
        |add_months(first_time,10)
        |from info
      """.stripMargin

    // 将数据写入数据库中
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/spark22"
    val table = "info"
    //ssc.sql(sql).write.jdbc(url, table, prop)
    //ssc.sql("select * from info").show(10)
    val res: DataFrame = ssc.sql("select * from info")
    val usr = "root"
    val pwd = "root"
    res.foreachPartition(part => {
      val list = new ListBuffer[TotalUser2DB]
      part.foreach(t => {
        //mac, phone_brand, enter_time, first_time,
        // last_time, region, screen, stay_long
        val mac = t.getString(0)
        val phone_brand = t.getString(1)
        val enter_time = t.getTimestamp(2)
        val first_time = t.getTimestamp(3)
        val last_time = t.getTimestamp(4)
        val region = t.getString(5)
        val screen = t.getString(6)
        val stay_long = t.getInt(7)
        list.append(TotalUser2DB(mac, phone_brand, enter_time, first_time, last_time, region, screen, stay_long))
      })
//      list.foreach(tup => {
//        println(tup.toString)
//      })
      insertInto(list, url, usr, pwd)
    })
    sc.stop()
    ssc.stop()
  }

  case class TotalUser2DB(mac: String, phone_brand: String, enter_time: Timestamp, first_time: Timestamp, last_time: Timestamp, region: String, screen: String, stay_long: Int) {
    override def toString: String = "list: " + mac + phone_brand + enter_time + first_time + last_time + region + screen + stay_long
  }

  // 插入数据库
  def insertInto(list: ListBuffer[TotalUser2DB], url: String, usr: String, pwd: String) = {
    var conn: Connection = ConnectionPool.getConnection
    var ps: PreparedStatement = null
    val sql = "insert into info(mac, phone_brand, enter_time, first_time,last_time,region, screen, stay_long) values(?,?,?,?,?,?,?,?)"
    try {
      Class.forName("com.mysql.jdbc.Driver")
      ps = conn.prepareStatement(sql)
      for (ele <- list) {
        ps.setString(1, ele.mac)
        ps.setString(2, ele.phone_brand)
        ps.setTimestamp(3, ele.enter_time)
        ps.setTimestamp(4, ele.first_time)
        ps.setTimestamp(5, ele.last_time)
        ps.setString(6, ele.region)
        ps.setString(7, ele.screen)
        ps.setInt(8, ele.stay_long)
        ps.addBatch()

      }
      ps.executeBatch()

    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      ps.close()
      conn.close()
    }

  }
    // 自定义UDF测试
    var res = ""

    def encryptMac(mac: String, key: String) = {
      res = AESUtil.encrypt(mac, key)
      res
    }

    def decryptMac(mac: String, key: String) = {
      res = AESUtil.decrypt(mac, key)
      res
    }


    // 定义方法,实现字符串util类型时间转换为sql类型时间
    def Str2Date(clo: String) = {
      val utildate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(clo)
      val sqldate = new Timestamp(utildate.getTime)
      sqldate
    }

}