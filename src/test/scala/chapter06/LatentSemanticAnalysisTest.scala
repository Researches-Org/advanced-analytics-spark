package chapter06

import org.scalatest.FunSuite

class LatentSemanticAnalysisTest extends FunSuite {

  test("termDocWeight") {
    val lsa = new LatentSemanticAnalysis(null)

    val weight = lsa.termDocWeight(1, 1, 1, 1)

    assert(weight == 0.0)
  }

}
