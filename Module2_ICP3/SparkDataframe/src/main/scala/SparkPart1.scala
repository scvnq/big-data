import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPart1 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    //TASK 1-1 Import the dataset and create data framesdirectly on import.
    val sc = SparkContext.getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.format("csv").option("header", "true").load("/Users/suannai/survey.csv")

    df.show()

    //TASK 1-2 Save data to file
    // df.write.format("com.databricks.spark.csv").save("/Users/suannai/Desktop/SparkDataframe/output")

    //TASK 1-3 Check for Duplicate records in the dataset
    df.createOrReplaceTempView("survey")
    val Duplicate= sqlContext.sql("select Country, Count(*) as Number from survey GROUP By Country Having Count(*)>1")
        Duplicate.show()

    //TASK1-4:Apply Union operation on the dataset and order the output by CountryName alphabetically
   println("Apply Union operation on the dataset and order the output by CountryName alphabetically")
    val df1 = sqlContext.sql("SELECT Country,Age FROM survey")
    val df2 =sqlContext.sql("SELECT treatment,family_history FROM survey")
    val unoinDf=df1.union(df2)
     unoinDf.show()
    unoinDf.orderBy("Country").show()
    //TASK1-5.Use Groupby Query based ontreatment
    val treatment=sqlContext.sql("select treatment,count(Country) as Number from survey GROUP BY treatment")
    treatment.show()

    //TASK2-1
     val df3=df.select("Country", "state","Age","Gender","Timestamp")
     val df4=df.select("Country","treatment","family_history","no_employees")
    df3.createOrReplaceTempView("df3")
     df4.createOrReplaceTempView("df4")
    val Join1=sqlContext.sql("select df3.Gender,df3.Age,df4.family_history,df4.treatment FROM df3,df4 where df3.Country=df4.Country ")
    Join1.show(numRows = 50)

     val Join2=sqlContext.sql("select df3.*, df4.* FROM df3 INNER JOIN df4 ON(df3.Country=df4.Country)")
    Join2.show(numRows = 10)
    //Aggregate functions

    val avgage = sqlContext.sql("SELECT Avg(Age) as AverageAge FROM survey")
    avgage.show()
    val maxage = sqlContext.sql("SELECT MAX(Age) as MaxAge FROM survey")
    maxage.show()
    val minage = sqlContext.sql("SELECT Min(Age) as MinAge FROM survey")
    minage.show()
    //fetch the 13th row
    val fetch13 = df.take(13).last
    print(fetch13)

    //bonus
    import spark.implicits._
    def parseLine(line: String): (String, String, String) = {
      val rows = line.split(",")
      val age = rows(1).toString
      val state = rows(4).toString
      val gender = rows(2).toString
      (age, state, gender)
    }
    val bonus = sc.textFile("/Users/suannai/survey.csv")
    val rdd=bonus.map(parseLine).toDF()
      rdd.show()



  }
}