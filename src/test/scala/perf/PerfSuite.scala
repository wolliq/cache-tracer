package perf

import com.amadeus.TestHelper
import org.scalatest.FunSuite
import perf.app.SparkPerfManager



class PerfSuite extends FunSuite with TestHelper{

  test("Should estimate RDD map info"){

    cores = 8.toString
    isLocal = true.toString
    testDatasetPath = "./src/test/resources/temp-nets/sx-stackoverflow.txt"

    val args = Array(cores, isLocal, testDatasetPath)

    SparkPerfManager.main(args)
  }
}
