package com.liucaijin.spark.jdbc.sqlserver

import org.apache.spark.SparkContext
import java.sql.Connection
import java.sql.DriverManager
 
import java.sql.PreparedStatement
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.log4j.Logger
import spark.CustomizedJdbcRDD
import spark.CustomizedJdbcPartition

object QuerySqlServerNoParm {
  
  
 def  sqlServerQuery(sc:SparkContext,sql:String):CustomizedJdbcRDD[Array[Object]]={
   
   val sparkConf=sc.getConf;
   
   val driver=sparkConf.get("spark.jdbc.driver")
   
   val DatabaseName=sparkConf.get("spark.jdbc.databasename")
   
   val driverURL=driver+";"+DatabaseName
   
   val jdbcuser=sparkConf.get("spark.jdbc.user")
   
   val jdbcpassword=sparkConf.get("spark.jdbc.password")
   
   val result=  new CustomizedJdbcRDD(sc,
                           //创建获取JDBC连接函数
                           () => {
 
     DriverManager.getConnection(driverURL, jdbcuser, jdbcpassword);
 
 
 
  },
  //设置查询SQL
  sql,
  //创建分区函数
  () => {
    val partitions=new Array[Partition](1);
    var parameters=Map[String, Object]();
//    parameters+=("host" -> "1");
    val partition=new CustomizedJdbcPartition(0, parameters);
    
    partitions(0)=partition;
     
    partitions;
  },
  //为每个分区设置查询条件(基于上面设置的SQL语句)
  (stmt:PreparedStatement, partition:CustomizedJdbcPartition) => {
//    stmt.setString(1, partition.asInstanceOf[CustomizedJdbcPartition]
//                               .partitionParameters.get("host").get.asInstanceOf[String])
    stmt;
  }
    );
   result
 }
  

}