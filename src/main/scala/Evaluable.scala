import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD

import scala.compat.Platform

/**
  * Created by zaoldyeck on 2016/3/2.
  */
trait Evaluable extends Serializable {
  def evaluate(model: MatrixFactorizationModel, testData: RDD[((Int, Int), Double)]): ConfusionMatrixResult = {
    val predictUsers: RDD[((Int, Int), Double)] = model.predict(testData.map(_._1)).map(rating => ((rating.user, rating.product), rating.rating))
    val predictResult: RDD[PredictResult] = predictUsers.join(testData).map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }
    predictResult.saveAsTextFile(s"als/evaluate/${Platform.currentTime}")
    calConfusionMatrix(predictResult)
  }

  def calConfusionMatrix(predictResult: RDD[PredictResult]): ConfusionMatrixResult = {
    case class ConfusionMatrix(tp: Double = 0, fp: Double = 0, fn: Double = 0, tn: Double = 0)

    val result: ConfusionMatrix = predictResult.map {
      case result: PredictResult if result.fact > 0 && result.predict >= 0.5 => ConfusionMatrix(tp = 1)
      case result: PredictResult if result.fact > 0 && result.predict < 0.5 => ConfusionMatrix(fn = 1)
      case result: PredictResult if result.fact <= 0 && result.predict >= 0.5 => ConfusionMatrix(fp = 1)
      case _ ⇒ ConfusionMatrix(tn = 1)
    }.reduce((sum, row) => ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))

    val p = result.tp + result.fn
    val n = result.fp + result.tn
    //Logger.log.warn("P=" + p)
    //Logger.log.warn("N=" + n)

    val mse: Double = predictResult.map(predictResult => Math.pow(predictResult.fact - predictResult.predict, 2)) mean()
    val accuracy = (result.tp + result.tn) / (p + n)
    val precision = result.tp / (result.tp + result.fp)
    val recall = result.tp / p
    val fallout = result.fp / n
    val sensitivity = result.tp / (result.tp + result.fn)
    val specificity = result.tn / (result.fp + result.tn)
    val f = 2 * ((precision * recall) / (precision + recall))
    ConfusionMatrixResult(mse, accuracy, precision, recall, fallout, sensitivity, specificity, f)
  }
}

case class ConfusionMatrixResult(mse: Double, accuracy: Double, precision: Double, recall: Double, fallout: Double, sensitivity: Double, specificity: Double, f: Double) {
  override def toString: String = {
    s"\n" +
      s"MSE = $mse\n" +
      s"Accuracy = $accuracy\n" +
      s"Precision = $precision\n" +
      s"Recall = $recall\n" +
      s"Fallout = $fallout\n" +
      s"Sensitivity = $sensitivity\n" +
      s"Specificity = $specificity\n" +
      s"F = $f"
  }

  def toListString: String = "%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f".format(mse, accuracy, precision, recall, fallout, sensitivity, specificity, f)
}

case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)