package converter
import com.github.nscala_time.time.Imports._
import scala.language.implicitConversions

/**
 * @author user
 */
object Helper {
  implicit def getSqlTimestamp(t: DateTime) = {
    new java.sql.Timestamp(t.getMillis)
  }

  implicit def getDateTime(st: java.sql.Timestamp) = {
    new DateTime(st)
  }

}