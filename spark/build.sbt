name := "InsightProject"
version := "1.0"
scalaVersion := "2.10.4"

val PhantomVersion = "1.22.0"

unmanagedJars in Compile := (baseDirectory.value ** "/home/ubuntu/insight-project/lib/spark-redis-0.1.0.jar").classpath


resolvers ++= Seq(
 "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
  "Twitter Repository"               at "http://maven.twttr.com",
  Resolver.bintrayRepo("websudos", "oss-releases")
)

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
"org.apache.spark" %% "spark-sql" % "1.4.0" % "provided",
"com.websudos" %% "phantom-dsl" % PhantomVersion,
"org.elasticsearch" % "elasticsearch-spark_2.10" % "2.3.2")

mergeStrategy in assembly := {

  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard

  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard

  case "log4j.properties"                                  => MergeStrategy.discard

  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines

  case "reference.conf"                                    => MergeStrategy.concat

  case _                                                   => MergeStrategy.first

}
