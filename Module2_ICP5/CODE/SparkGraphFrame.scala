import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.struct
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.graphframes._

object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    // 1.Import the dataset as a csv file and create data framesdirectly on importthan create graph out of the data frame created.
    var df1 = spark.read.format("csv").option("header", "true").load("/Users/suannai/Desktop/SparkGraphframe/201508_trip_data.csv").toDF();
    var station = spark.read.format("csv").option("header", "true").load("/Users/suannai/Desktop/SparkGraphframe/201508_station_data.csv").toDF();
    var df = spark.read.format("csv").option("header", "true").load("/Users/suannai/Desktop/SparkGraphframe/201508_trip_data.csv");
    df.printSchema();
    df.toDF().show();

    // 2.Concatenate chunks into list & convert to DataFrame
    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val task2 = station.select(col("station_id"),col("name"),lit(" "))
      task2.toDF.show()

   // 3.Remove duplicates
    var distinct = df1.distinct().show();
   // 4.Name Columns
    var newname = df.withColumnRenamed("Start Date","Begindate").distinct().show();
   // Creating the graph.
    var edges = df1.withColumnRenamed("End Terminal","dst").withColumnRenamed("Start Terminal","src").withColumnRenamed("Subscriber Type","relationship");
    //val g = GraphFrame(station.withColumnRenamed("station_id","id"), edges);
  //  g.vertices.show(false);
    //g.edges.show(false);

  // g.inDegrees.show(false);
   // g.outDegrees.show(false);

   // val motifs = g.find("(50)-[e]->(70); (70)-[e2]->(501)").show(false)


    val TripData = df.withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")
    val StationData = station.distinct()
      .withColumnRenamed("station_id", "id")
    /*
        val Data = GraphFrame(StationData, TripData)
        println("Total Number of Stations: " + Data.vertices.count)
        println("Total Number of Trips in Graph: " + Data.edges.count)

        //	Vertex in-Degree
        val in_Degree = Data.inDegrees
        in_Degree.show(5)

        //Vertex out-Degree
        val out_Degree = Data.outDegrees
        out_Degree.show()

        //Apply the motif findings
        val motifs: DataFrame = Data.find("(a)-[e]->(b); (b)-[e2]->(a)")
        motifs.show()
        */

    // 6.	Create vertices
    val Data = GraphFrame(StationData, TripData)

    // 7.	Show some vertices
    println("Stations' amount: " + Data.vertices.count)

    // 8.	Show some edges
    println("Trip's amount: " + Data.edges.count)

    // 9.	Vertex in-Degree
    val in_Degree = Data.inDegrees
    in_Degree.show(5)

    // 10.	Vertex out-Degree
    val out_Degree = Data.outDegrees
    out_Degree.show()

    // 11.	Apply the motif findings
    val motifs: DataFrame = Data.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()
  }
}
