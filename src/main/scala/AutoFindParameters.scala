import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel, ALS}
import org.apache.spark.rdd.RDD

import scala.compat.Platform

/**
  * Created by zaoldyeck on 2016/3/2.
  */
class AutoFindParameters(implicit sc: SparkContext) extends Serializable {
  /*
  private val evaluatePath: String = "evaluate"
  private val dataTransformer: DataTransformable = new DataTransformable("parquet.`/user/kigo/dataset/modeling_data`")
  private val allData: RDD[Rating] = dataTransformer.getLoginData.sample(withReplacement = false, 0.1, Platform.currentTime)
  private val splitData: Array[RDD[Rating]] = allData.randomSplit(Array(0.8, 0.2), Platform.currentTime)
  private val trainingData: RDD[Rating] = splitData(0).persist
  private val testData: RDD[((Int, Int), Double)] = trainingData.sample(withReplacement = false, 0.2, Platform.currentTime).map(rating => ((rating.user, rating.product), rating.rating)).persist
  private val validateData: RDD[((Int, Int), Double)] = splitData(1).map(rating => ((rating.user, rating.product), rating.rating)).persist

  findParameters(0.1, 100)

  def findParameters(alpha: Double, lambda: Double): Unit = {
    val model: MatrixFactorizationModel = ALS.train(trainingData, 30, 20)
    //val model: MatrixFactorizationModel = ALS.trainImplicit(trainingData, 30, 20)
    //      new ALS()
    //        .setImplicitPrefs(true)
    //        .setRank(30)
    //        .setIterations(20)
    //        .setAlpha(alpha)
    //        .setLambda(lambda)
    //        .setNonnegative(true)
    //        .run(trainingData)

    val header: String = "%.4f,%.4f".format(alpha, lambda)

    val testDataMatrixResult: ConfusionMatrixResult = Evaluable.evaluate(model, testData)
    writeFile(evaluatePath, header + testDataMatrixResult.toListString)
    val validateDataMatrixResult: ConfusionMatrixResult = Evaluable.evaluate(model, validateData)
    writeFile(evaluatePath, header + validateDataMatrixResult.toListString)

    if (testDataMatrixResult.f < 0.9) findParameters(alpha + 0.3, lambda)
    else if (testDataMatrixResult.f - validateDataMatrixResult.f > 0.1) findParameters(alpha, lambda + 10)
  }
  */
}