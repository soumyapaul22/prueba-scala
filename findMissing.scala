import scala.collection.mutable.ListBuffer
import scala.io.StdIn.readInt

object findMissing {
  def createInputSet(inputLength:Integer):Set[Int]={
    var n = 0
    var inpList: ListBuffer[Int] = new ListBuffer()
    println("Please provide the Inputs as Int :")
    while (n < inputLength) {
    try {
      val input = readInt.toInt
      if (input > 0 & input <=100){
        inpList += input}
      else{
        println("Input should be between 1 to 100, Input rejected")
        n=n-1
      }
    }
    catch {
      case e: Exception => println("Non Integer Input . Error " + e.getMessage)
        n=n-1
    }
    n = n + 1
  }
    val inpSet:Set[Int] = inpList.toSet
    return (inpSet)
  }

  def identifyMissing(inpSet:Set[Int]):List[Int]={
    val mainSet= (1 to 100).toSet
    val sizeOfSet = inpSet.size
    println(s"Total number of elements in Set is $sizeOfSet")
    val diff = mainSet.diff(inpSet)
    val diffList = diff.toList.sorted
    return (diffList)
  }
}
