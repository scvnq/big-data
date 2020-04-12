import org.apache.spark._
object SecondarySorting {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark - Secondary Sort").setMaster("local")
    val sc =new SparkContext(conf)
    val personRDD =sc.textFile("input1")
val pairRDD =personRDD.map(_.split(",")).map{k => (k(0),k(1))}
    val numReducers = 2;
    val listRDD = pairRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
 val resultRDD =listRDD.flatMap{
   case (labe1,list) => {

    list.map((labe1, _))
   }
 }
    resultRDD.saveAsTextFile("output1")


  }

}
