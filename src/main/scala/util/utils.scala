package util

class Utils {
  def getKeyFromLine(dataLine: String): String = {
    dataLine.substring(0, 10)
  }

  def getRandomTempKey(): String = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to 20) {
      sb.append(r.nextInt())
    }
    sb.toString
  }
}
