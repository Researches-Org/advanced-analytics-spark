package chapter08

import org.apache.spark.sql.Row

class RichRow(row: Row) {

  def getAs[T](field: String): Option[T] = {

    if (row.isNullAt(row.fieldIndex(field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }

  }

}
