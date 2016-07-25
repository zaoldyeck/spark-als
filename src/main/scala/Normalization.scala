import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/4/12.
  */
class Normalization(implicit @transient override val sc: SparkContext) extends Spark with DataTransformable with Serializable {
  private[this] var _schema: String = _
  private[this] var _mode: String = _
  private[this] var _path: String = _

  override def schema: String = _schema

  def mode: String = _mode

  def path: String = _path

  def setSchema(schema: String): this.type = {
    _schema = schema
    this
  }

  def setMode(mode: String): this.type = {
    _mode = mode
    this
  }

  def setPath(path: String): this.type = {
    _path = path
    this
  }

  def run(): Unit = {
    val normalizedData: RDD[Rating] = mode match {
      case Constants.MODE.SAVING => normalize(getRawSavingData).persist
      case Constants.MODE.LOGIN => getLoginData.persist
    }

    val model: MatrixFactorizationModel = new ALS()
      .setRank(60)
      .setIterations(100)
      .setNonnegative(true)
      .setImplicitPrefs(true)
      .run(normalizedData)

    //ALS.train(normalizedData, 60, 50)

    val process: String = s"hadoop fs -rm -f -r $path"
    process.!

    model.save(sc, path)
  }
}