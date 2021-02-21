package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupInfo")
  var xyzData = spark.sql("select * from pickupInfo where((x >= "+minX+" AND x <= "+maxX+") AND (y >= "+minY+" AND y <= "+maxY+") AND (z >= "+minZ+" AND z <= "+maxZ+"))")
  xyzData.createOrReplaceTempView("xyzData")

  var cellTable = spark.sql("select x as i, y as j, z as k, count(*) as count from xyzData group by i,j,k")

  cellTable.createOrReplaceTempView("ijk")
  cellTable.show()

  var meanDF = spark.sql("select (SUM(ijk.count)/"+numCells+") as mean from ijk")
  val mean = meanDF.first()(0).toString.toDouble

  var s_value = spark.sql("select sqrt((SUM(ijk.count*ijk.count)/"+numCells+") - " + mean * mean + ") from ijk")
  s_value.show()
  val s_val = s_value.first()(0).toString.toDouble

  spark.udf.register("GetWN",(i:Int, j:Int, k:Int)=>(
    HotcellUtils.GetNeighbourCount(i, minX, maxX, j, minY, maxY, k, minZ,maxZ)
    ))

  var neighDF = spark.sql("SELECT ijk1.i, ijk1.j, ijk1.k, ijk1.count as myX, SUM(ijk2.count) as neighxsum, count(ijk2.i, ijk2.j, ijk2.k) as totalneighcount FROM ijk ijk1, ijk ijk2 WHERE ((abs(ijk1.i-ijk2.i)<=1) AND (abs(ijk1.j-ijk2.j)<=1) AND (abs(ijk1.k-ijk2.k)<=1)) GROUP BY ijk1.i, ijk1.j, ijk1.k, ijk1.count")
  neighDF.show()
  neighDF.createOrReplaceTempView("semifinal")


  var g_valueDF = spark.sql("SELECT semifinal.i, semifinal.j, semifinal.k FROM semifinal order by ((semifinal.neighxsum -("+mean+"*semifinal.totalneighcount))/("+s_val+"*sqrt((("+numCells.toInt+"*semifinal.totalneighcount)-(semifinal.totalneighcount*semifinal.totalneighcount))/("+numCells.toInt+"-1)))) desc, semifinal.i desc, semifinal.j desc, semifinal.k desc")
  g_valueDF.show()

  return g_valueDF
}
}
