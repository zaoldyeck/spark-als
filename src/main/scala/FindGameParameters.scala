import java.util.concurrent.Executors

import com.pubgame.alphakigo.AlphakiGo
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/3/22.
  */
class FindGameParameters(@transient implicit override val sc: SparkContext) extends RandomFindParameters {
  private[this] var _gameId: Int = _

  def gameId: Int = _gameId

  def setGameId(value: Int): this.type = {
    _gameId = value
    this
  }

  override def run(): Unit = {
    val allData: RDD[Rating] = mode match {
      case Constants.MODE.SAVING => getRawSavingData.persist//getSavingData.persist
      case Constants.MODE.LOGIN => getLoginTimeData.persist//getLoginData.persist
    }

    val alphakiGo = AlphakiGo.怒開一個
    alphakiGo.speakAtChannel("targeting", s"Game $gameId find parameters start !")

    case class Parameters(rank: Int, lambda: Double, alpha: Double)

    val parametersSeq: IndexedSeq[Parameters] = for {
      rank <- 2 until 100 by 1
      lambda <- 0.01 until 2 by 0.01
      alpha <- 0.1 until 1 by 0.01
    } yield Parameters(rank, lambda, alpha)

    val executorService: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))
    val futures: IndexedSeq[Future[Unit]] = Random.shuffle(parametersSeq).map {
      parameters => Future {
        val gameData: RDD[Rating] = allData.filter(_.product == gameId)
        val nonGameData: RDD[Rating] = allData.filter(_.product != gameId)
        val splitData: Array[RDD[Rating]] = gameData.randomSplit(Array(0.8, 0.2), Platform.currentTime)
        val trainingData: RDD[Rating] = (nonGameData union splitData(0)).persist
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
            alphakiGo.speakAtChannel("targeting", content)
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