name := "testScala"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

libraryDependencies += "org.scalanlp" % "breeze-math_2.10" % "0.4"
libraryDependencies  += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies  +=  "org.scalanlp" %% "breeze-natives" % "0.13.2"
libraryDependencies  += "org.scalanlp" %% "breeze-viz" % "0.13.2"

//libraryDependencies += "org.vegas-viz" %% "vegas" % {vegas-version}
//libraryDependencies += "co.theasi" %% "plotly" % "0.2.0"
//libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.9"
//libraryDependencies += "org.vegas-viz" %% "vegas-spark" % "0.3.9"