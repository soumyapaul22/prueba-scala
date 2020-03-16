import scala.collection.mutable.ListBuffer
import scala.io.StdIn.readInt

object findMissing {
  def createInputSet(inputLength:Integer, maxLength:Int):Set[Int]={
    var n = 0
    var inpList: ListBuffer[Int] = new ListBuffer()
    var inpSet: Set[Int] = Set(0)
    println("Please provide the Inputs as Int :")
    while (n < inputLength) {
    try {
      val input = readInt.toInt
      if (input > 0 & input <=maxLength){
        inpList += input}
      else{
        println(s"Input should be between 1 to $maxLength, Input rejected")
        n=n-1
      }
    }
    catch {
      case e: Exception => println("Non Integer Input . Error " + e.getMessage)
        n=n-1
    }
    n = n + 1
  }
    val tempInpSet:Set[Int] = inpList.toSet
    if (tempInpSet.size > maxLength) {
      inpSet = setLengthValidation(tempInpSet, maxLength)
    } else {
      inpSet = tempInpSet
    }
    println(inpSet)
    return (inpSet)
  }

  def setLengthValidation(tempInpSet:Set[Int],maxLength:Int):Set[Int]={
    var tempList: List[Int] = List(0)
    println(s"Size is more than $maxLength, so process will only consider first $maxLength in Ascending Order")
    tempList = tempInpSet.toList.sorted.slice(0,maxLength)
    val inpSet:Set[Int] = tempList.toSet
    return(inpSet)
  }

  def identifyMissing(inpSet:Set[Int],maxLength:Int):List[Int]={
    val mainSet= (1 to maxLength).toSet
    val sizeOfSet = inpSet.size
    println(s"Total number of elements in Set is $sizeOfSet")
    val diff = mainSet.diff(inpSet)
    val diffList = diff.toList.sorted
    return (diffList)
  }
}
