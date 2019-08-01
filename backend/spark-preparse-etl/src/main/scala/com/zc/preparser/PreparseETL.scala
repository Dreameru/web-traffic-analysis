package com.zc.preparser

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import preparse.{PreParsedLog, WebLogPreParser}

object PreparseETL {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("PreparseETL")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val rawdataInputPath=spark.conf.get("spark.traffic.analysis.rawdata.input",
      "hdfs://master:9999/user/hadoop-zc/traffic-analysis/rawlog/weblog-20180615.txt")  //读取配置

    val numberPartitions=spark.conf.get("spark.traffic.analysis.rawdata.numberPartitions","2").toInt //存储分区数设置

    val preParseLogRDD=spark.sparkContext.textFile(rawdataInputPath)
      .flatMap(line=>Option(WebLogPreParser.parse(line)))

    val preParsedLogDS=spark.createDataset(preParseLogRDD)(Encoders.bean(classOf[PreParsedLog]))//回头复习一下如何创建dataset


    preParsedLogDS.coalesce(numberPartitions)  //处理小文件，将数据合并
      .write
      .mode(SaveMode.Append)
      .partitionBy("year","month","day")
      .saveAsTable("rawdata.web")

    spark.stop()

  }
}
