package chapter02

case class Score(value: Double) {

  def +(oi: Option[Int]) = {
    Score(value + oi.getOrElse(0))
  }

}

