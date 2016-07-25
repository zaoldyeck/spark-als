import java.util.concurrent.Executors

import com.google.common.util.concurrent.AtomicDouble
import com.pubgame.alphakigo.AlphakiGo
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.immutable.IndexedSeq
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/3/14.
  */
class RandomFindParameters(implicit @transient override val sc: SparkContext) extends Spark with DataTransformable with Evaluable with Exportable with Serializable {
  private[this] var _schema: String = _
  private[this] var _path: String = _
  private[this] var _mode: String = _
  private[this] var _lastF: AtomicDouble = _

  override def schema: String = _schema

  override def path: String = _path

  def mode: String = _mode

  def lastF: AtomicDouble = _lastF

  def setSchema(schema: String): this.type = {
    _schema = schema
    this
  }

  def setPath(path: String): this.type = {
    _path = path
    this
  }

  def setMode(mode: String): this.type = {
    _mode = mode
    this
  }

  def setLastF(lastF: Double): this.type = {
    _lastF = new AtomicDouble(lastF)
    this
  }

  @tailrec
  protected final def compareAndSet(newV: Double): Boolean = {
    val oldV = lastF.get()
    if (newV > oldV)
      if (lastF.compareAndSet(oldV, newV)) true
      else compareAndSet(newV)
    else false
  }

  def run(): Unit = {
    val allData: RDD[Rating] = mode match {
      case Constants.MODE.SAVING => getSavingData.persist
      case Constants.MODE.LOGIN => getLoginData.persist
    }

    val alphakiGo = AlphakiGo.怒開一個

    case class Parameters(rank: Int, lambda: Double, alpha: Double)

    val parametersSeq: IndexedSeq[Parameters] = for {
      rank <- 2 until 100 by 1
      lambda <- 0.01 until 100 by 0.03
      alpha <- 0.01 until 0.3 by 0.03
    } yield Parameters(rank, lambda, alpha)

    val executorService: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))
    val futures: IndexedSeq[Future[Unit]] = Random.shuffle(parametersSeq).map {
      case parameters => Future {
        val splitData: Array[RDD[Rating]] = allData.randomSplit(Array(0.8, 0.2), Platform.currentTime)
        val trainingData: RDD[Rating] = splitData(0).persist
        val testData: RDD[((Int, Int), Double)] = splitData(1).map(rating => ((rating.user, rating.product), rating.rating)).persist

        try {
          val model: MatrixFactorizationModel = ALS.trainImplicit(trainingData, parameters.rank, 50, parameters.lambda, parameters.alpha)

          val header: String = "%3d,% 2.2f,%.2f".format(parameters.rank, parameters.lambda, parameters.alpha)

          val testDataMatrixResult: ConfusionMatrixResult = evaluate(model, testData)
          val thisF: Double = testDataMatrixResult.f
          val content: String = header + "  " + testDataMatrixResult.toListString
          Logger.log.warn(content)

          if (compareAndSet(thisF)) {
            writeFile(content)
            alphakiGo.speakToPeople("vincent", content)
          }
        } catch {
          case e: Exception => Logger.log.error(e.getMessage)
        } finally {
          trainingData.unpersist()
          testData.unpersist()
        }
      }(executorService)
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }
}