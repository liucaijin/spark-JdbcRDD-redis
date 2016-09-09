package com.liucaijin.spark.mainfunc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar
import java.sql.Timestamp
import java.sql.Date
 
import java.math.BigDecimal
import com.redislabs.provider.redis._
import org.apache.spark.SparkEnv
import com.liucaijin.spark.jdbc.sqlserver.QuerySqlServerNoParm
/**
 * 
 */
object NavLJSYL {
  def getUnitNV(a: ((Integer, Integer, Option[Date]), (Object, Double)), b: ((Integer, Integer, Option[Date]), (Object, Double))): ((Integer, Integer, Option[Date]), (Object, Double)) = {

    val date = if (a._1._3 == None) { (None, 1.0) }
    else {
      val endDate = a._1._3.get
//      val endDateTemp = a._2._1.asInstanceOf[Date]
      
       val endDateTemp =if( a._2._1.isInstanceOf[Date]) a._2._1.asInstanceOf[Date]
                        else new Date (a._2._1.asInstanceOf[Timestamp].getTime)
                        

      val endDateTemp2 = new Date(b._2._1.asInstanceOf[Timestamp].getTime)

      if (endDateTemp.equals(endDate)) (Some(endDateTemp), a._2._2)
      else if (endDateTemp2.equals(endDate)) (Some(endDateTemp2), b._2._2)
      else if (endDateTemp.before(endDate) && endDateTemp2.before(endDate)) {
        if (endDateTemp.after(endDateTemp2)) (Some(endDateTemp), a._2._2)
        else (Some(endDateTemp2), b._2._2)
      } else if (endDateTemp.before(endDate)) (Some(endDateTemp), a._2._2)
      else if (endDateTemp2.before(endDate)) (Some(endDateTemp2), b._2._2)
      else (None, 1.0)

      //           val lessEndDateTemp=if(endDateTemp.after(endDateTemp2)) (endDateTemp2,b._2._2)
      //                               else (endDateTemp,a._2._2)
      //         if(endDate.equals(endDateTemp))   (endDateTemp,a._2._2)
      //           else  if(lessEndDateTemp._1) 
    }
    val dateTemp = if (date._1 != None) {
      date._1.get
    } //如果为-1则表示日期为空
    else new Date(-1);
    ((a._1._1, a._1._2, a._1._3), (dateTemp, date._2))
  }
  
   

  def getNewRdd(a: (Integer, ((Integer, Option[Object]), Option[Integer]))): (Integer, (Integer, Integer)) = {
    val innerCode = a._1
    val fundCode = a._2._1._1
    val relatedCode = a._2._1._2 match {
      case Some(x) => {
        x match {
          case null => a._2._2 match {
            case Some(x) => x
            case _ => innerCode
          }
          case _=>innerCode
        }
      }
      case None => innerCode
      case _ => innerCode
    }
    (relatedCode, (innerCode, fundCode))
  }

  //得到前n个月的日期
  def getNMonthBeforeDate(n: Int, date: java.sql.Date): java.sql.Date = {
    var calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, -n)
    new java.sql.Date(calendar.getTime().getTime)
  }
  //将日期加n天
  def getAddNDay(n: Int, date: java.sql.Date): java.sql.Date = {
    var calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, n);
    new java.sql.Date(calendar.getTime().getTime)
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HdfsWordCount20160818") //.setMaster("spark://10.1.51.88:7077")
      .setMaster("local[2]")
      .set("redis.host", "hostid")
      .set("redis.port", "port")
      .set("redis.auth", "password")
      .set("redis.db", "1")
    val sc = new SparkContext(sparkConf)
    
    val sparkEnv=SparkEnv.get    
    val sql = "select  distinct C.RelatedCode,B.InnerCode from " +
      "VT_FundBasicInfo B  join SecuMain_TZZD C  on B.InnerCode=C.InnerCode " +
      "join MF_FundArchives D on C.RelatedCode=D.InnerCode " +
      "left join MF_FundArchivesAttach E on E.InnerCode=C.RelatedCode " +
      "where     (D.FundType<>'1109' and E.TypeCode<>'55')"

    val data1 = QuerySqlServerNoParm.sqlServerQuery(sc, sql)

    val sql2 = "select InnerCode,EndDate,UnitNV from MF_NetValue  where EndDate is not null"

    val data2 = QuerySqlServerNoParm.sqlServerQuery(sc, sql2)

    val sql3 = "select InnerCode,GrowthRateFactor,ExDiviDate from MF_AdjustingFactor where ExDiviDate is not null"

    val data3 = QuerySqlServerNoParm.sqlServerQuery(sc, sql3)

    val sql4 = "select InnerCode innerCode,EndDate endDate from" +
      "(select rn=ROW_NUMBER()over(partition by B.InnerCode order by B.EndDate desc),B.* " +
      "from(select InnerCode,EndDate from MF_NetValue )B)C where rn=1"

    val data4 = QuerySqlServerNoParm.sqlServerQuery(sc, sql4)

    val sql5 = "select InnerCode,EstablishmentDate  from MF_FundArchives"

    val data5 = QuerySqlServerNoParm.sqlServerQuery(sc, sql5)

    val sql6 = "select InnerCode,RelatedInnerCode from MF_CodeRelationship"

    val data6 = QuerySqlServerNoParm.sqlServerQuery(sc, sql6)

    val sql7 = "select InnerCode,ParValue  from MF_IssueAndListing"

    val data7 = QuerySqlServerNoParm.sqlServerQuery(sc, sql7)
    //基金外部代码和基金内码，前一个为secuMain中的对应代码，后一个为secuMain_TZZD中的基金内码
    val innercodeAndFundInnercode = data1.map { x => (x.apply(0).asInstanceOf[Integer], x.apply(1).asInstanceOf[Integer]) }.cache
    
    innercodeAndFundInnercode.foreach(f=>println(f._1))
 
    val mf_NetValue = data2.map { x => (x.apply(0).asInstanceOf[Integer], (x.apply(1), x.apply(2).asInstanceOf[BigDecimal].doubleValue())) }.cache()

    val mf_AdjustingFactor = data3.map { x => (x.apply(0).asInstanceOf[Integer], (x.apply(2), x.apply(1).asInstanceOf[Double])) }.cache()

    //输入的截止日期
    val endDateRDD = data4.map { x => (x.apply(0).asInstanceOf[Integer], x.apply(1)) }.cache

    //基金成立日期
    val mf_FundArchives = data5.map { x => (x.apply(0).asInstanceOf[Integer], x.apply(1)) }.cache
    
    mf_FundArchives.foreach(f=>println(f._1))

    val mf_CodeRelationship = data6.map { x => (x.apply(0).asInstanceOf[Integer], x.apply(1).asInstanceOf[Integer]) }.cache

    val MF_IssueAndListing = data7.map { x => (x.apply(0).asInstanceOf[Integer], x.apply(1).asInstanceOf[BigDecimal].doubleValue()) }.cache
    
    MF_IssueAndListing.collect()
    //输入的开始日期
    val startDateRDD = endDateRDD.map(f => {
      if (f._2 == null) {
        (f._1, None)
      } else {
        //加0天是因为起始日期需要-1向前找（约束EndDate＝"开始日期"-１(无记录向前找)，提取UnitNV）
        (f._1, Some(getAddNDay(0, getNMonthBeforeDate(1, new Date(f._2.asInstanceOf[Timestamp].getTime)))))
      }
    })

    val unionRDD = innercodeAndFundInnercode.leftOuterJoin(mf_FundArchives)

    val unionRDD2 = unionRDD.leftOuterJoin(mf_CodeRelationship)
    //替换后的innercode组合，第三个为fundCode，存到redis以fundCode为主键
    val relatedCode = unionRDD2.map(getNewRdd)
    //最好用leftOuterJoin 防止relatedCode因为join莫名消失
    val relatedCodeAndSatrtDate = relatedCode.leftOuterJoin(mf_FundArchives)

    val startDateRDD2 = relatedCodeAndSatrtDate.join(startDateRDD)
    startDateRDD2.collect()
    //最终得到的输入的开始日期 和对应的innercode     
    val startDateRDD3 = startDateRDD2.map(f => {
      //  (f._2._1._2,f._2._2) 前一个为成立日期，后一个为开始日期
      val date: Option[Date] = if (f._2._1._2 == None) Some(f._2._2.get)
      else if (f._2._2 == None) None
      else if (new Date(f._2._1._2.get.asInstanceOf[Timestamp].getTime).after(f._2._2.get)) Some(new Date(f._2._1._2.get.asInstanceOf[Timestamp].getTime))
      else Some(f._2._2.get)

      (f._1, (f._2._1._1._1, f._2._1._1._2, date))
    })
    startDateRDD3.foreach(f=>println("开始日期："+f))
    //最终得到的截止日期对应的innercode
    val endDateRdd2 = relatedCode.join(endDateRDD)
    endDateRdd2.foreach(f=>println("endDateRdd2:"+f))
    //将endDateRdd2中日期为空的date转化成Option类型
    val endDateRdd3 = endDateRdd2.map(f => {
      val date = if (f._2._2 == null) {
        None
      } else  Some(new Date(f._2._2.asInstanceOf[Timestamp].getTime))

      (f._1, (f._2._1._1, f._2._1._2, date))
    })

    val startDateRDD4 = startDateRDD3.join(mf_NetValue)

    //约束EndDate＝"开始日期"-１(无记录向前找)，提取UnitNV
    val startDateRDD5 = startDateRDD4.reduceByKey(getUnitNV)
    startDateRDD5.foreach(f=>println("单位净值"+f))

    val startDateRDD6 = startDateRDD5.join(MF_IssueAndListing).map(f => {
      val unitVN = if (f._2._1._2._1.asInstanceOf[Date].getTime == -1) f._2._2
      else { f._2._1._2._2 }
      (f._1, (f._2._1._1._1, f._2._1._1._2, unitVN))
    })
    //开始日期 innercode 对应的基金因子
    val innercodeStartDateFactor = startDateRDD3.join(mf_AdjustingFactor).reduceByKey(getUnitNV).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2._2)))

    val endDateRdd4 = endDateRdd3.join(mf_NetValue).reduceByKey(getUnitNV)
    
    val endDateRdd5 = endDateRdd4.join(MF_IssueAndListing).map(f => {
      val unitVN = if (f._2._1._2._1.asInstanceOf[Date].getTime == -1) f._2._2
      else { f._2._1._2._2 }
      (f._1, (f._2._1._1._1, f._2._1._1._2, unitVN))
    })

    val innercodeEndDateFactor= endDateRdd3.join(mf_AdjustingFactor).reduceByKey(getUnitNV).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2._2)))
       
    //将由于join操作丢失掉的innercode找回
   val endDateRdd6= endDateRdd5.leftOuterJoin(innercodeEndDateFactor).map(f=>{
     val unitNv=  if( f._2._2==None) f._2._1._3
                   else f._2._1._3*(f._2._2.get._3)
                  
          (f._1,(f._2._1._1,f._2._1._2,unitNv))         
      })
    
  val startDateRDD7=  startDateRDD6.leftOuterJoin(innercodeStartDateFactor).map(f=>{
     val unitNv=  if( f._2._2==None) f._2._1._3
                   else f._2._1._3*(f._2._2.get._3)
                  
          (f._1,(f._2._1._1,f._2._1._2,unitNv))         
      })
    
 val oneYearLJSYL=  endDateRdd6.join(startDateRDD7).map(f=>{
     if(f._2._2._3==0.0){
       (f._2._1._2+"","")
     }else{
        (f._2._1._2+"",f._2._1._3/f._2._2._3-1+"")
     } 
    })
    oneYearLJSYL.collect
    sc.toRedisHASH(oneYearLJSYL,"oneYearLJSYL")//存储到redis
    oneYearLJSYL.foreach(f=>println(f))
//    endDateRdd6.leftOuterJoin(oneYearLJSYL).map(f=>{
//      if(f._2._2==None) println("不能得到基金的开始日期,对应的基金代码为:"+f._2._1._2)
//    })
  }
}