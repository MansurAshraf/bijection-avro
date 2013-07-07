import sbt._
import sbt.Keys._

object BijectionavroBuild extends Build {

  lazy val bijectionavro = Project(
    id = "bijection-avro",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "bijection-avro",
      organization := "org.github.mansur",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.3",
      libraryDependencies ++= Seq(
        "org.apache.avro" % "avro" % "1.7.4",
        "com.novocode" % "junit-interface" % "0.10-M1" % "test",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources(),
        "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" withSources()
      ),
      libraryDependencies += "com.twitter" %% "bijection-core" % "0.5.0" excludeAll(
        ExclusionRule(organization = "org.scalacheck"),
        ExclusionRule(organization = "org.scala-tools.testing")
        ),
      resolvers ++= Seq(
        "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
        "releases" at "http://oss.sonatype.org/content/repositories/releases"
      )
    )
  )
}
