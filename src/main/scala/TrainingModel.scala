import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/3/21.
  */
class TrainingModel(@transient implicit override val sc: SparkContext) extends Spark with DataTransformable {
  private[this] var _schema: String = _
  private[this] var _mode: String = _
  private[this] var _rank: Int = _
  private[this] var _iterations: Int = _
  private[this] var _lambda: Double = _
  private[this] var _alpha: Double = _
  private[this] var _path: String = _

  override def schema: String = _schema

  def mode: String = _mode

  def rank: Int = _rank

  def iterations: Int = _iterations

  def lambda: Double = _lambda

  def alpha: Double = _alpha

  def path: String = _path

  def setSchema(schema: String): this.type = {
    _schema = schema
    this
  }

  def setMode(mode: String): this.type = {
    _mode = mode
    this
  }

  def setRank(rank: Int): this.type = {
    _rank = rank
    this
  }

  def setIterations(iterations: Int): this.type = {
    _iterations = iterations
    this
  }

  def setLambda(lambda: Double): this.type = {
    _lambda = lambda
    this
  }

  def setAlpha(alpha: Double): this.type = {
    _alpha = alpha
    this
  }

  def setPath(path: String): this.type = {
    _path = path
    this
  }

  def run(): Unit = {
    val allData: RDD[Rating] = mode match {
      case Constants.MODE.SAVING => getSavingData.persist
      case Constants.MODE.LOGIN => getLoginData.persist
    }

    val model: MatrixFactorizationModel = ALS.trainImplicit(allData, rank, iterations, lambda, alpha)

    val process: String = s"hadoop fs -rm -f -r $path"
    process.!

    model.save(sc, path)
  }
}