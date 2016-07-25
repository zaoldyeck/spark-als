import org.apache.log4j.{Logger, Level, LogManager}

/**
  * Created by zaoldyeck on 2015/12/29.
  */
object Logger extends Serializable {
  @transient lazy val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.WARN)
}