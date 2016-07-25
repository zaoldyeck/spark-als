import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

trait Spark {
  def sc: SparkContext
  val sqlContext = SQLContext.getOrCreate(sc)
}