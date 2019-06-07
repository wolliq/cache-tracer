import sbt.Keys.{libraryDependencies, _}
import sbt._

scalaVersion := Versions.scala

organization := "stef"

name := "cache-tracer"

version := (version in ThisBuild).value

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
scalacOptions += "-target:jvm-1.8"

javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  // COMMON
  "org.scalactic" %% "scalactic" % Versions.scalatest,
  "org.scalatest" %% "scalatest" % Versions.scalatest % "test",
  // SPARK
  "org.apache.spark" %% "spark-core" % Versions.spark,//% "provided",
  "org.apache.spark" %% "spark-sql" % Versions.spark,//% "provided",
  "org.apache.spark" %% "spark-mllib" % Versions.spark,//% "provided",
  "org.apache.spark" %% "spark-graphx" % Versions.spark)//% "provided")

