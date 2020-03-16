import scala.util.control.Breaks._

class findMissingCls {
  def missingSet(inputLength:Integer):List[Int]={
    var diffList: List[Int] = List(0)
    breakable {
      if (inputLength < 0 | inputLength > 100) {
        println("list is out of range, value should be between 1 to 100")
        break}
      else{
        val inpSet = findMissing.createInputSet(inputLength)
        diffList = findMissing.identifyMissing(inpSet)
        val diffSize = diffList.size
        println(s"Mising values for 1 to 100 : $diffSize")
        println(diffList)
      }
    }
    return (diffList)
  }
}
