package chapter08

class RichRow(row: org.apache.spark.sql.Row) {

  def getAs[T](field: String): Option[T] = {

    if (row.isNullAt(row.fieldIndex(field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }

  }

}
