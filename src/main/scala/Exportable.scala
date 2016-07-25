import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/3/2.
  */
trait Exportable extends Serializable {
  self: Spark =>

  def path: String

  //private[this] lazy val os: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path, true))
  //sys.addShutdownHook(os.close())

  def writeFile(content: String): Unit = {
    /*
    try {
      os.write(s"$content\n".getBytes)
    } catch {
      case e: Exception => Logger.log.error(e.printStackTrace())
    } finally os.flush()
    */

    val printWriter: PrintWriter = new PrintWriter(new FileOutputStream(path, true))
    try {
      printWriter.append(content)
      printWriter.println()
    } catch {
      case e: Exception => Logger.log.error(e.printStackTrace())
    } finally printWriter.close()
  }

  case class User(player: Int, game: Int, pred: Double)

  def saveUser(ratings: RDD[Rating]): Unit = {
    import sqlContext.implicits._
    ratings.map(rating => User(rating.user, rating.product, rating.rating)).toDF.registerTempTable("User")

    val process: String = s"hadoop fs -rm -f -r $path"
    process.!

    sqlContext.sql("select * from User").repartition(1).write.save(path)
  }
}