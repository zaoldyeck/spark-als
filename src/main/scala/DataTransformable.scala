import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created by zaoldyeck on 2016/3/2.
  */
trait DataTransformable extends Serializable {
  self: Spark =>

  def schema: String

  def getSavingData: RDD[Rating] = {
    sqlContext.sql(s"SELECT user_id,game_id,save_sum_game FROM $schema WHERE web_mobile=2 and area='港臺'").rdd map {
      case Row(user_id: Long, game_id: Int, save_sum_game) =>
        Rating(user_id.toInt, game_id, if (Some(save_sum_game).getOrElse(0).asInstanceOf[Int] > 0) 1 else 0)
    }
  }

  def getRawSavingData: RDD[Rating] = {
    sqlContext.sql(s"SELECT user_id,game_id,save_sum_game FROM $schema WHERE web_mobile=2 and area='港臺'").rdd map {
      case Row(user_id: Long, game_id: Int, save_sum_game) =>
        Rating(user_id.toInt, game_id, Some(save_sum_game).getOrElse(0).asInstanceOf[Int])
    }
  }

  def getLoginData: RDD[Rating] = {
    sqlContext.sql(s"SELECT user_id,game_id,login_days_game FROM $schema WHERE web_mobile=2 and area='港臺'").rdd map {
      case Row(user_id: Long, game_id: Int, login_days_game) =>
        Rating(user_id.toInt, game_id, if (Some(login_days_game).getOrElse(0).asInstanceOf[Int] >= 7) 1 else 0)
    }
  }

  def getLoginTimeData: RDD[Rating] = {
    sqlContext.sql(s"SELECT user_id,game_id,login_times_game FROM $schema WHERE web_mobile=2 and area='港臺'").rdd map {
      case Row(user_id: Long, game_id: Int, login_times_game) =>
        Rating(user_id.toInt, game_id, if (Some(login_times_game).getOrElse(0).asInstanceOf[Int] >= 2) 1 else 0)
    }
  }

  def getPlayersIdByGameId(gameId: Int): Broadcast[Array[Int]] = {
    val playersId: Array[Int] = sqlContext.sql(s"SELECT distinct user_id FROM $schema WHERE game_id=$gameId").rdd map {
      case Row(user_id: Long) => user_id.toInt
    } collect()
    sc.broadcast(playersId)
  }

  def getPredictData(gameId: Int): RDD[(Int, Int)] = {
    sqlContext.sql(s"SELECT a.user_id FROM " +
      s"(SELECT DISTINCT user_id FROM $schema WHERE game_id != $gameId and web_mobile=2 and area='港臺') AS a " +
      s"LEFT JOIN " +
      s"(SELECT DISTINCT user_id FROM $schema WHERE game_id = $gameId and web_mobile=2 and area='港臺') AS t " +
      s"ON a.user_id = t.user_id WHERE t.user_id IS NULL") map {
      case Row(user_id: Long) => (user_id.toInt, gameId)
    }
  }

  def normalize(data: RDD[Rating]): RDD[Rating] = data.map(rating => Rating(rating.user, rating.product, Math.log(rating.rating + 1)))
}