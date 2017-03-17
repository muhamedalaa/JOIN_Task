package com.project_team.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  
  case class calls_details(user:Long, other:String, direction:String, duration:Int ,timestamp:String)
  case class message_details(user:Long, other:String, direction:String, message_Length:Int ,timestamp:String)

  def mapper(line:String): calls_details = {
    val fields = line.split(',')  
    
    val CRD:calls_details = calls_details(fields(0).toLong, fields(1).toString, fields(2).toString, fields(3).toInt,fields(4).toString)
    return CRD
  }
  
  def mapper2(line:String): message_details = {
    val fields = line.split(',')  
    
    val CRD_with_message:message_details = message_details(fields(0).toLong, fields(1).toString, fields(2).toString, fields(3).toInt,fields(4).toString)
    return  CRD_with_message
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("../calls.csv")
     val calls_record = lines.map(mapper)
     val lines2 = spark.sparkContext.textFile("../messages.csv")
    val message_record = lines2.map(mapper2)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemacall = calls_record.toDS
    val schemamsg = message_record.toDS
    
    schemacall.createOrReplaceTempView("calls_record")
    
     schemamsg.createOrReplaceTempView("message_record")
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT calls_record.user ,calls_record.other ,calls_record.direction,calls_record.duration,calls_record.timestamp ,message_record.message_Length from calls_record JOIN message_record ON calls_record.user = message_record.user").distinct().show()
    
   
    
    spark.stop()
  }
}