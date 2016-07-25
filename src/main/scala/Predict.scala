import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by zaoldyeck on 2016/3/23.
  */
class Predict(@transient implicit override val sc: SparkContext) extends Spark with DataTransformable with Exportable with Serializable {
  private[this] var _schema: String = _
  private[this] var _path: String = _
  private[this] var _modelPath: String = _
  private[this] var _gmeId: Int = _

  override def schema: String = _schema

  override def path: String = _path

  def modelPath: String = _modelPath

  def gameId: Int = _gmeId

  def setSchema(schema: String): this.type = {
    _schema = schema
    this
  }

  def setPath(path: String): this.type = {
    _path = path
    this
  }

  def setModelPath(modelPath: String): this.type = {
    _modelPath = modelPath
    this
  }

  def setGameId(gameId: Int): this.type = {
    _gmeId = gameId
    this
  }

  def run(): Unit = {
    val model: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, modelPath)
    val predictData: RDD[(Int, Int)] = getPredictData(gameId)
    val user: RDD[Rating] = model.recommendUsers(44,5000)//model.predict(predictData).filter(_.rating > 0)
    saveUser(user)
  }
}