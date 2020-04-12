import org.apache.spark.{SparkConf, SparkContext}
object mergeSort {
val conf= new SparkConf()
  conf.setMaster("local").setAppName("jsonfile")
  val sc = new SparkContext(conf)
  def msort[T](less:(T,T)=>Boolean)(xs:List[T]):List[T]={
    def merge(xs:List[T], ys:List[T]): List[T]={
      (xs,ys) match {
        case(Nil, _) => ys
        case(_,Nil) => xs
          case(x :: xs1, y :: ys1) =>
          if(less (x,y)) x :: merge(xs1,ys)
          else y :: merge(xs, ys1)

      }
    }
    val n =xs.length/2
    if (n == 0) xs
    else {
      val (ys, zs)= xs.splitAt(n)
      merge(msort(less)(ys),msort(less)(zs))
    }
  }

  def main(args: Array[String]): Unit = {
    val data= List(38,27,43,3,9,82,10)
    def compareFunc(left: Int, right: Int): Boolean ={
      left <= right
    }
    val result = sc.parallelize(msort(compareFunc)(data))
    for (x<-result){
      println(x + "\t")
    }
  }



}
