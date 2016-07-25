name := "spark-als"

version := "1.0"

crossScalaVersions := Seq("2.10.6", "2.11.8")

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "latest.integration" % Provided,
  "org.apache.spark" %% "spark-mllib" % "latest.integration" % Provided,
  "org.apache.spark" %% "spark-hive" % "latest.integration" % Provided,
  "com.github.scopt" %% "scopt" % "latest.integration",
  "com.pubgame" %% "alphakigo" % "latest.integration"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Pubgame Data Team Maven" at "http://ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com/repository/maven",
  Resolver.url("Pubgame Data Team SBT", url("http://ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com/repository/sbt"))(Resolver.ivyStylePatterns)
)