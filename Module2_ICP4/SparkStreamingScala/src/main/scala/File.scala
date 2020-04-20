import java.io._
import scala.io.Source
import scala.util.Random

object File {

  def main(args: Array[String]): Unit = {
    var a = 1


    while(a<=30){
      var ran=Random.nextInt(12)+1
      val filename = Source.fromFile("/Users/suannai/Desktop/SparkStreamingScala/sb.txt")
      val file = "/Users/suannai/Desktop/SparkStreamingScala/log/log" + a + ".txt"
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      var buffer = filename.getLines()
      var nextlines=""
      var i=0;
      while(i<ran){
        nextlines =nextlines+ buffer.next() + "\n"
        i=i+1
      }
      writer.write(nextlines + "\n")
      writer.close()
      a = a+1;
      Thread.sleep(4000)
    }

  }
}
