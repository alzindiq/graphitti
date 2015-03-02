name := "graphitti"

version := "0.1"

organization := "com.hp.hpl"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val gremlinScalaV = "3.0.0.M6c"
  val gremlinV = "3.0.0.M6"
  val scalatestV = "2.2.1"
  val log4jV = "1.2.15"
  Seq(
    "com.michaelpollmeier" %% "gremlin-scala" % gremlinScalaV exclude("org.slf4j", "slf4j-log4j12"),
    "com.tinkerpop" % "gremlin-core" % gremlinV exclude("org.slf4j", "slf4j-log4j12"),
    "com.tinkerpop" % "neo4j-gremlin" % gremlinV,
    "commons-io" % "commons-io" % "2.4",
    "org.joda" % "joda-convert" % "1.7",
    "joda-time" % "joda-time" % "2.4",
    "org.scalatest" %% "scalatest" % scalatestV % "test",
    "log4j" % "log4j" % log4jV excludeAll(
      ExclusionRule(organization = "com.sun.jdmk"),
      ExclusionRule(organization = "com.sun.jmx"),
      ExclusionRule(organization = "javax.jms"))
  )
}

resolvers ++= Seq(
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Ansvia repo" at "http://scala.repo.ansvia.com/releases/",
  "Neo4jContrib repo" at "https://github.com/neo4j-contrib/m2/raw/master/snapshots"
)