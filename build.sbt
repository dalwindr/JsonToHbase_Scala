name := "json_toHbase_tosql"

version := "0.1"

  scalaVersion := "2.11.12"
 //   scalaVersion := "2.12.8" 
//val sparkVersion = "2.4.0"


// javaVersion := "1.8"
// hbaseVersion := "1.4.9"

val jsonlatestVersion="3.6.5"
val json4sNative = "org.json4s" %% "json4s-native" % jsonlatestVersion
val json4sJackson = "org.json4s" %% "json4s-jackson" % jsonlatestVersion
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += json4sNative
libraryDependencies += json4sJackson
libraryDependencies += "org.json4s" %% "json4s-ext" % jsonlatestVersion

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.8"
