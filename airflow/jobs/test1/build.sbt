ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "test1",
    idePackagePrefix := Some("org.test.app")
  )
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.6"  //% "provided"
// https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.5-bundle
libraryDependencies += "org.apache.hudi" %% "hudi-spark3.5-bundle" % "0.15.0"  //% "provided"

// https://mvnrepository.com/artifact/io.prestosql/presto-jdbc
libraryDependencies += "io.prestosql" % "presto-jdbc" % "350"

// https://mvnrepository.com/artifact/io.trino/trino-jdbc
libraryDependencies += "io.trino" % "trino-jdbc" % "422"

libraryDependencies += "com.typesafe" % "config" % "1.4.3"