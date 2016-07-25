import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by zaoldyeck on 2016/3/2.
  */
object Main {
  self: Spark =>

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.instances", "10")
      //.set("spark.dynamicAllocation.maxExecutors", "10")
      .registerKryoClasses(
      Array(
        classOf[DataTransformable],
        classOf[Evaluable],
        classOf[Exportable],
        classOf[RandomFindParameters],
        classOf[AutoFindParameters],
        classOf[TrainingModel]))

    val parser = new scopt.OptionParser[Config]("als") {
      head("als", "0.2")
      opt[String]('a', "action").required.action {
        (value, config) => config.copy(action = value)
      } text "action is an required string property"

      opt[String]('m', "mode").required.action {
        (value, config) => config.copy(mode = value)
      } text "mode is an required string property"

      opt[String]('s', "schema").action {
        (value, config) => config.copy(schema = value)
      } text "schema is an string property"

      opt[String]('p', "path").action {
        (value, config) => config.copy(path = value)
      } text "path is an string property"

      opt[Double]('f', "lastF").action {
        (value, config) => config.copy(lastF = value)
      } text "f is an double property"

      opt[Int]('g', "gameId").action {
        (value, config) => config.copy(gameId = value)
      } text "gameId is an integer property"

      opt[Int]('r', "rank").action {
        (value, config) => config.copy(rank = value)
      } text "rank is an integer property"

      opt[Int]('i', "iterations").action {
        (value, config) => config.copy(iterations = value)
      } text "iterations is an integer property"

      opt[Double]('l', "lambda").action {
        (value, config) => config.copy(lambda = value)
      } text "lambda is an double property"

      opt[Double]("al").abbr("alpha").action {
        (value, config) => config.copy(alpha = value)
      } text "alpha is an double property"

      opt[String]("mp").abbr("modelPath").action {
        (value, config) => config.copy(modelPath = value)
      } text "mp is string double property"
    }

    parser.parse(args.map(_.toLowerCase), Config()) match {
      case Some(config) => config.action match {
        case Constants.ACTION.TRAINING =>
          conf.setAppName(s"ALS training ${config.gameId} model ${config.mode} mode")

          run {
            implicit sc =>
              val path: String = if (config.path.isEmpty) s"als/model/${config.gameId}/${config.mode}" else config.path

              new TrainingModel()
                .setSchema(config.schema)
                .setMode(config.mode)
                .setRank(config.rank)
                .setIterations(config.iterations)
                .setLambda(config.lambda)
                .setAlpha(config.alpha)
                .setPath(path)
                .run()
          }
        case Constants.ACTION.PREDICT =>
          conf.setAppName(s"ALS predict ${config.gameId} users ${config.mode} mode")

          run {
            implicit sc =>
              val path: String = if (config.path.isEmpty) s"als/predict/${config.gameId}/${config.mode}" else config.path
              val modelPath: String = if (config.modelPath.isEmpty) s"als/model/${config.gameId}/${config.mode}" else config.modelPath

              new Predict()
                .setSchema(config.schema)
                .setPath(path)
                .setModelPath(modelPath)
                .setGameId(config.gameId)
                .run()
          }
        case Constants.ACTION.NORMAL=>
          conf.setAppName(s"ALS training model use normalized data")

          run {
            implicit sc =>
              val path: String = if (config.path.isEmpty) s"als/model/normalization/${config.mode}" else config.path

              new Normalization()
                .setSchema(config.schema)
                .setMode(config.mode)
                .setPath(path)
                .run()
          }
        case action =>
          conf
            .set("spark.executor.cores", "16")
            .set("spark.executor.memory", "24576m")
            .set("spark.yarn.executor.memoryOverhead", "8192")

          val path: String = if (config.path.isEmpty) "evaluate" else config.path

          action match {
            case Constants.ACTION.FIND_ALL =>
              conf.setAppName(s"ALS find general parameters ${config.mode} mode")

              run {
                implicit sc =>
                  new RandomFindParameters()
                    .setSchema(config.schema)
                    .setPath(path)
                    .setMode(config.mode)
                    .setLastF(config.lastF)
                    .run()
              }
            case Constants.ACTION.FIND_GAME =>
              conf.setAppName(s"ALS find ${config.gameId} parameters ${config.mode} mode")
              //              implicit val sc: SparkContext = new SparkContext(conf)

              run {
                implicit sc =>
                  new FindGameParameters()
                    .setSchema(config.schema)
                    .setPath(path)
                    .setMode(config.mode)
                    .setLastF(config.lastF)
                    .setGameId(config.gameId)
                    .run()
              }
          }
      }
      case None =>
    }

    def run(callback: SparkContext => Unit): Unit = {
      val sc: SparkContext = new SparkContext(conf)
      sc.setCheckpointDir("checkpoint")
      callback(sc)
      sc.stop
    }
  }
}

case class Config(action: String = "",
                  mode: String = "",
                  schema: String = "parquet.`/user/kigo/data0411/result/by_user`",
                  path: String = "",
                  lastF: Double = 0D,
                  gameId: Int = -1,
                  rank: Int = -1,
                  iterations: Int = 50,
                  lambda: Double = -1,
                  alpha: Double = -1,
                  modelPath: String = "")