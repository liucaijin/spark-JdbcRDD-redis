package spark

import scala.reflect.ClassTag
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import java.sql.ResultSet
import java.sql.Connection
import org.apache.spark.Partition
import org.apache.spark.Logging
import java.sql.PreparedStatement
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.function.{Function => JFunction}

class CustomizedJdbcPartition(idx: Int, parameters: Map[String, Object]) extends Partition {
  override def index = idx
  val partitionParameters=parameters
}
// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * An RDD that executes an SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 * @param lowerBound the minimum value of the first placeholder
 * @param upperBound the maximum value of the second placeholder
 *   The lower and upper bounds are inclusive.
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class CustomizedJdbcRDD[T:ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    getCustomizedPartitions: () => Array[Partition],
    prepareStatement: (PreparedStatement, CustomizedJdbcPartition) => PreparedStatement, 
    mapRow: (ResultSet) => T = CustomizedJdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {


  override def getPartitions: Array[Partition] = {
    getCustomizedPartitions();
  }


  override def compute(thePart: Partition, context: TaskContext) = new  NextIterator[T] {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val part = thePart.asInstanceOf[CustomizedJdbcPartition]
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)


    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    try {
   if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
     stmt.setFetchSize(Integer.MIN_VALUE)
     logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
   }
    } catch {
    case ex: Exception => {
        //ex.printStackTrace();
      }
    }


    prepareStatement(stmt, part)
    
    val rs = stmt.executeQuery()


    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }


    override def close() {
      try {
        if (null != rs && ! rs.isClosed()) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt && ! stmt.isClosed()) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && ! conn.isClosed()) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}


object CustomizedJdbcRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }


  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }


  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results.
   * For usage example, see test case JavaAPISuite.testJavaJdbcRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query must contain two ? placeholders for parameters used to partition the results.
   *   E.g. "select title, author from books where ? <= id and id <= ?"
   * @param lowerBound the minimum value of the first placeholder
   * @param upperBound the maximum value of the second placeholder
   *   The lower and upper bounds are inclusive.
   * @param numPartitions the number of partitions.
   *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
   *   the query would be executed twice, once with (1, 10) and once with (11, 20)
   * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
   *   This should only call getInt, getString, etc; the RDD takes care of calling next.
   *   The default maps a ResultSet to an array of Object.
   */
  def create[T](
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      getCustomizedPartitions: () => Array[Partition],
      prepareStatement: (PreparedStatement, CustomizedJdbcPartition) => PreparedStatement, 
      mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {


    val jdbcRDD = new CustomizedJdbcRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      getCustomizedPartitions,
      prepareStatement,
      (resultSet: ResultSet) => mapRow.call(resultSet))(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])


    new JavaRDD[T](jdbcRDD)(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
  }


  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results. Each row is
   * converted into a `Object` array. For usage example, see test case JavaAPISuite.testJavaJdbcRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query must contain two ? placeholders for parameters used to partition the results.
   *   E.g. "select title, author from books where ? <= id and id <= ?"
   * @param lowerBound the minimum value of the first placeholder
   * @param upperBound the maximum value of the second placeholder
   *   The lower and upper bounds are inclusive.
   * @param numPartitions the number of partitions.
   *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
   *   the query would be executed twice, once with (1, 10) and once with (11, 20)
   */
  def create(
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      getCustomizedPartitions: () => Array[Partition],
      prepareStatement: (PreparedStatement, CustomizedJdbcPartition) => PreparedStatement): JavaRDD[Array[Object]] = {


    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }


    create(sc, connectionFactory, sql, getCustomizedPartitions, prepareStatement, mapRow)
  }
}