import org.scalacheck.Gen



object Generadores {


  val genCharList = Gen.containerOf[List,Char](Gen.choose('a','z'))
  val genIntList = Gen.containerOf[List, Int](Gen.choose(20,50))
  val genStringList = Gen.containerOf[List, String](Gen.alphaLowerStr.retryUntil(p => p.length == 3))
  val genEvenList = Gen.containerOf[List, Int](Gen.choose(1,50).map(n => n*2))


  def main(args: Array[String]): Unit = {
    println(genCharList.retryUntil(p => p.length == 10).sample)
    println(genIntList.sample)
    println(genStringList.sample)
    println(genEvenList.retryUntil(p => p.length == 20).sample)
  }

}