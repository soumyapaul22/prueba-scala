import scala.io.StdIn.readInt

object findMissingSpec {
  def main(args: Array[String]): Unit = {
    println("Please provide total number of set you want to build :")
    val inputLength = readInt.toInt
    val findMissingIns = new findMissingCls
    val missingVal = findMissingIns.missingSet(inputLength)
  }
}
