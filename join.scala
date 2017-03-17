package dakak.work

import org.apache.spark.SparkContext
import org.apache.log4j._

object join {
  
   def select_column(line: String):Int = {
    val x = line.split(",")   //return array
    val y =x.length          // return length of array
    println("please select column for join")
   var a=scala.io.StdIn.readInt()
    while(a < y-1  || a<0  ) 
    {
        println("try again") 
       a=scala.io.StdIn.readInt()
    } // end of while
    return a
  }  // end of select_column()

 
  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","join")
   
    val line =sc.textFile("../calls.csv")
    val offset_of_table1=select_column(line.toString())
    val table1 =line.map(x =>(x.split(",")(offset_of_table1),x))
    
    val line2=sc.textFile("../messages.csv")
      val offset_of_table2=select_column(line2.toString())
     val table2 =line.map(x =>(x.split(",")(offset_of_table2),x))  
    table1.join(table2).foreach(println) 
  
    
  }
}