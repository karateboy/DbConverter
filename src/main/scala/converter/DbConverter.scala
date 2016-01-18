package converter
import scalikejdbc._
import com.github.nscala_time.time.Imports._

object RecordType extends Enumeration {
  val Air = Value
  val AirHr = Value
  val Climate = Value
  val ClimateHr = Value
}

object TabType extends Enumeration {
  val Hour = Value
  val Min = Value
  val SixSec = Value
}

object Monitor extends Enumeration {
  val A001 = Value
  val A002 = Value
  val A003 = Value
  val A004 = Value
  val A005 = Value
  val A006 = Value
  val A007 = Value
  val A008 = Value
  val A009 = Value
  val A010 = Value
  val A011 = Value
  val A012 = Value
  val A013 = Value
}

/**
 * @author user
 */
object DbConverter {
  def setupDB = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    ConnectionPool.add('src, "jdbc:sqlserver://localhost\\AQMS-SERVER:1433;databaseName=AQMSDB",
      "sa", "@tlas1302")

    ConnectionPool.add('dest, "jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databaseName=AQMSDB",
      "sa", "@tlas1302")

  }

  def getTabName(rt: RecordType.Value, year: Int) = {
    val tab =
      rt match {
        case RecordType.AirHr     => SQLSyntax.createUnsafely(s"A_AVGHR${year - 1911}")
        case RecordType.Air       => SQLSyntax.createUnsafely(s"A_AVGR${year - 1911}")
        case RecordType.ClimateHr => SQLSyntax.createUnsafely(s"C_AVGHR${year - 1911}")
        case RecordType.Climate   => SQLSyntax.createUnsafely(s"C_AVGR${year - 1911}")
      }
    tab
  }

  case class OldRecord(monitor: Monitor.Value, monitorType: String, timestamp: DateTime, value: Float, status: String)

  def getOneDaySixSecRecord(rt: RecordType.Value, start: DateTime) = {
    val tab = getTabName(rt, start.getYear)
    NamedDB('src) readOnly { implicit session =>
      sql"""
        Select *
        FROM ${tab}
        Where M_Year = ${start.getYear - 1911} and M_Month = ${start.getMonthOfYear} and M_Day = ${start.getDayOfMonth} 
          and Item in ('C911', 'C912')
        """.map { r =>
        val (year, month, day, time) = (r.int(4), r.int(5), r.int(6), r.string(7))
        val timestamp = DateTime.parse(s"${year + 1911}-${month}-${day}-${time}", DateTimeFormat.forPattern("YYYY-M-d-HHmmss"))
        OldRecord(Monitor.withName(r.string(2)), r.string(3), timestamp, r.float(8), r.string(9))
      }.list().apply()
    }
  }

  def getOneDayMinRecord(rt: RecordType.Value, start: DateTime) = {
    val tab = getTabName(rt, start.getYear)
    NamedDB('src) readOnly { implicit session =>
      sql"""
        Select *
        FROM ${tab}
        Where M_Year = ${start.getYear - 1911} and M_Month = ${start.getMonthOfYear} and M_Day = ${start.getDayOfMonth} 
          and M_Time like '____00'
        """.map { r =>
        val (year, month, day, time) = (r.int(4), r.int(5), r.int(6), r.string(7))
        val timestamp = DateTime.parse(s"${year + 1911}-${month}-${day}-${time}", DateTimeFormat.forPattern("YYYY-M-d-HHmmss"))
        OldRecord(Monitor.withName(r.string(2)), r.string(3), timestamp, r.float(8), r.string(9))
      }.list().apply()
    }
  }

  def getOneMonthRecord(rt: RecordType.Value, start: DateTime) = {
    val tab = getTabName(rt, start.getYear)
    NamedDB('src) readOnly { implicit session =>
      sql"""
        Select *
        FROM ${tab}
        Where M_Year = ${start.getYear - 1911} and M_Month = ${start.getMonthOfYear}
        """.map { r =>
        val (year, month, day, time) = (r.int(4), r.int(5), r.int(6), r.string(7))
        val timestamp = DateTime.parse(s"${year + 1911}-${month}-${day}-${time}", DateTimeFormat.forPattern("YYYY-M-d-HHmmss"))
        OldRecord(Monitor.withName(r.string(2)), r.string(3), timestamp, r.float(8), r.string(9))
      }.list().apply()
    }
  }

  val mtList = List(
    "A213",
    "A214",
    "A215",
    "A221",
    "A222",
    "A223",
    "A224",
    "A225",
    "A226",
    "A229",
    "A232",
    "A233",
    "A235",
    "A283",
    "A286",
    "A288",
    "A289",
    "A293",
    "A296",
    "C211",
    "C212",
    "C213",
    "C214",
    "C215",
    "C216")

  val sixSecMtList = List("C911", "C912")
  
  import scala.collection.mutable.Map
  def writeHourRecord(recordMap: Map[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]], year: Int) {
    val timeList = recordMap.keySet.toList.sorted
    val monitorList = Monitor.values.toList.sorted
    val tab = SQLSyntax.createUnsafely(s"P1234567_Hr_${year}")

    val data =
      for {
        t <- timeList
        m <- monitorList
        hrMap = recordMap(t).getOrElse(m, Map.empty[String, (Option[Float], Option[String])])
      } yield {
        import scala.collection.mutable.ArrayBuffer
        val hr = ArrayBuffer.empty[Any]
        hr.append(m.toString)
        hr.append(t)
        hr.append(None)
        for (mt <- mtList) {
          val mtData = hrMap.getOrElse(mt, (None, None))
          hr.append(mtData._1)
          hr.append(mtData._2)
        }
        hr.toSeq
      }

    NamedDB('dest) localTx { implicit session =>
      sql"""
        Insert into ${tab}(
        [DP_NO]
      ,[M_DateTime]
      ,[CHK]
      ,[A513V]
      ,[A513S]
      ,[A514V]
      ,[A514S]
      ,[A515V]
      ,[A515S]
      ,[A521V]
      ,[A521S]
      ,[A522V]
      ,[A522S]
      ,[A523V]
      ,[A523S]
      ,[A524V]
      ,[A524S]
      ,[A525V]
      ,[A525S]
      ,[A526V]
      ,[A526S]
      ,[A529V]
      ,[A529S]
      ,[A532V]
      ,[A532S]
      ,[A533V]
      ,[A533S]
      ,[A535V]
      ,[A535S]
      ,[A583V]
      ,[A583S]
      ,[A586V]
      ,[A586S]
      ,[A588V]
      ,[A588S]
      ,[A589V]
      ,[A589S]
      ,[A593V]
      ,[A593S]
      ,[A596V]
      ,[A596S]
      ,[C511V]
      ,[C511S]
      ,[C512V]
      ,[C512S]
      ,[C513V]
      ,[C513S]
      ,[C514V]
      ,[C514S]
      ,[C515V]
      ,[C515S]
      ,[C516V]
      ,[C516S]
        ) 
        values (
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?)
        """
        .batch(data: _*)
        .apply()
    }
  }

  def writeMinRecord(recordMap: Map[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]], year: Int) {
    val timeList = recordMap.keySet.toList.sorted
    val monitorList = Monitor.values.toList.sorted
    val tab = SQLSyntax.createUnsafely(s"P1234567_M1_${year}")

    val data =
      for {
        t <- timeList
        m <- monitorList
        hrMap = recordMap(t).getOrElse(m, Map.empty[String, (Option[Float], Option[String])])
      } yield {
        import scala.collection.mutable.ArrayBuffer
        val hr = ArrayBuffer.empty[Any]
        hr.append(m.toString)
        hr.append(t)
        hr.append(None)
        for (mt <- mtList) {
          val mtData = hrMap.getOrElse(mt, (None, None))
          hr.append(mtData._1)
          hr.append(mtData._2)
        }
        hr.toSeq
      }

    NamedDB('dest) localTx { implicit session =>
      sql"""
        Insert into ${tab}(
        [DP_NO]
      ,[M_DateTime]
      ,[CHK]
      ,[A213V]
      ,[A213S]
      ,[A214V]
      ,[A214S]
      ,[A215V]
      ,[A215S]
      ,[A221V]
      ,[A221S]
      ,[A222V]
      ,[A222S]
      ,[A223V]
      ,[A223S]
      ,[A224V]
      ,[A224S]
      ,[A225V]
      ,[A225S]
      ,[A226V]
      ,[A226S]
      ,[A229V]
      ,[A229S]
      ,[A232V]
      ,[A232S]
      ,[A233V]
      ,[A233S]
      ,[A235V]
      ,[A235S]
      ,[A283V]
      ,[A283S]
      ,[A286V]
      ,[A286S]
      ,[A288V]
      ,[A288S]
      ,[A289V]
      ,[A289S]
      ,[A293V]
      ,[A293S]
      ,[A296V]
      ,[A296S]
      ,[C211V]
      ,[C211S]
      ,[C212V]
      ,[C212S]
      ,[C213V]
      ,[C213S]
      ,[C214V]
      ,[C214S]
      ,[C215V]
      ,[C215S]
      ,[C216V]
      ,[C216S]) 
        values (
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?)
        """
        .batch(data: _*)
        .apply()
    }
  }

  def get10SecList(begin:DateTime)={
    import scala.collection.mutable.ListBuffer
    val end = begin + 1.minute
    val buffer = ListBuffer.empty[DateTime]
    var current = begin
    while(current < end){
      buffer.append(current)
      current += 6.second
    }
    
    buffer.toList
  }
  
  def writeSixSecRecord(recordMap: Map[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]], start:DateTime) {
    val timeList = recordMap.keySet.toList.sorted
    val monitorList = Monitor.values.toList.sorted
    val tab = SQLSyntax.createUnsafely(s"P1234567_S6_${start.getYear}")
    val end = start + 1.day
    var current = start


    def getData(m:Monitor.Value, mt:String, time:DateTime):(Option[Float], Option[String])={
      val mMapOpt = recordMap.get(time)
      if(mMapOpt.isEmpty)
        return (None, None)
        
      val mtMapOpt = mMapOpt.get.get(m)
      if(mtMapOpt.isEmpty)
        return (None, None)
      else{
        val mtMap = mtMapOpt.get
        return mtMap.getOrElse(mt, (None, None))
      }
    }
    
    import scala.collection.mutable.ListBuffer
    val recordBuffer = ListBuffer.empty[Seq[Any]] 

    while(current < end){
      for{
        m<-monitorList
      }{
        import scala.collection.mutable.ArrayBuffer
        val rec = ArrayBuffer.empty[Any]
        rec.append(m.toString)
        rec.append(current)
        rec.append(None)

        for{
          mt<-sixSecMtList
          t<-get10SecList(current)
        }{
          val data = getData(m, mt, t)
          rec.append(data._1)
          rec.append(data._2)
        }
        recordBuffer.append(rec)
      }
      current = current + 1.minute
    }
    
    val data =
      for {
        t <- timeList
        m <- monitorList
        hrMap = recordMap(t).getOrElse(m, Map.empty[String, (Option[Float], Option[String])])
      } yield {
        import scala.collection.mutable.ArrayBuffer
        val hr = ArrayBuffer.empty[Any]
        hr.append(m.toString)
        hr.append(t)
        hr.append(None)
        for (mt <- sixSecMtList) {
          val mtData = hrMap.getOrElse(mt, (None, None))
          hr.append(mtData._1)
          hr.append(mtData._2)
        }
        hr.toSeq
      }

    NamedDB('dest) localTx { implicit session =>
      sql"""
        Insert into ${tab}(
        [DP_NO]
      ,[M_DateTime]
      ,[CHK]
      ,[C911_0V]
      ,[C911_0S]
      ,[C911_1V]
      ,[C911_1S]
      ,[C911_2V]
      ,[C911_2S]
      ,[C911_3V]
      ,[C911_3S]
      ,[C911_4V]
      ,[C911_4S]
      ,[C911_5V]
      ,[C911_5S]
      ,[C911_6V]
      ,[C911_6S]
      ,[C911_7V]
      ,[C911_7S]
      ,[C911_8V]
      ,[C911_8S]
      ,[C911_9V]
      ,[C911_9S]
      ,[C912_0V]
      ,[C912_0S]
      ,[C912_1V]
      ,[C912_1S]
      ,[C912_2V]
      ,[C912_2S]
      ,[C912_3V]
      ,[C912_3S]
      ,[C912_4V]
      ,[C912_4S]
      ,[C912_5V]
      ,[C912_5S]
      ,[C912_6V]
      ,[C912_6S]
      ,[C912_7V]
      ,[C912_7S]
      ,[C912_8V]
      ,[C912_8S]
      ,[C912_9V]
      ,[C912_9S]) 
        values (
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?)
        """
        .batch(recordBuffer: _*)
        .apply()
    }
  }

  def convertHourRecordForOneMonth(start: DateTime) = {

    val airRecord = getOneMonthRecord(RecordType.AirHr, start)
    val climateRecord = getOneMonthRecord(RecordType.ClimateHr, start)
    val allRecord = airRecord ++ climateRecord
    val recordMap = Map.empty[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]]
    for {
      r <- allRecord
    } {
      val hrMap = recordMap.getOrElse(r.timestamp, {
        val map = Map.empty[Monitor.Value, Map[String, (Option[Float], Option[String])]]
        recordMap.put(r.timestamp, map)
        map
      })
      val monitorMap = hrMap.getOrElse(r.monitor,
        {
          val map = Map.empty[String, (Option[Float], Option[String])]
          hrMap.put(r.monitor, map)
          map
        })
      monitorMap.put(r.monitorType, (Some(r.value), Some(r.status)))
    }

    Console.println("Total #=" + recordMap.size)
    try {
      writeHourRecord(recordMap, start.getYear)
    } catch {
      case e: Exception =>
        Console.println("Ignore exception...")
    }
  }

  def convertMinRecordForOneDay(start: DateTime) = {

    val airRecord = getOneDayMinRecord(RecordType.Air, start)
    val climateRecord = getOneDayMinRecord(RecordType.Climate, start)
    val allRecord = airRecord ++ climateRecord
    val recordMap = Map.empty[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]]
    for {
      r <- allRecord
    } {
      val minMap = recordMap.getOrElse(r.timestamp, {
        val map = Map.empty[Monitor.Value, Map[String, (Option[Float], Option[String])]]
        recordMap.put(r.timestamp, map)
        map
      })
      val monitorMap = minMap.getOrElse(r.monitor,
        {
          val map = Map.empty[String, (Option[Float], Option[String])]
          minMap.put(r.monitor, map)
          map
        })
      monitorMap.put(r.monitorType, (Some(r.value), Some(r.status)))
    }

    Console.println("Total #=" + recordMap.size)
    Console.println("Flush record to new table...")
    try {
      writeMinRecord(recordMap, start.getYear)
    } catch {
      case e: Exception =>
        Console.print("Ignore update exception...")
    }
  }

  def convertSixSecRecordForOneDay(start: DateTime) = {
    val allRecord = getOneDaySixSecRecord(RecordType.Climate, start)
    val recordMap = Map.empty[DateTime, Map[Monitor.Value, Map[String, (Option[Float], Option[String])]]]
    for {
      r <- allRecord
    } {
      val minMap = recordMap.getOrElse(r.timestamp, {
        val map = Map.empty[Monitor.Value, Map[String, (Option[Float], Option[String])]]
        recordMap.put(r.timestamp, map)
        map
      })
      val monitorMap = minMap.getOrElse(r.monitor,
        {
          val map = Map.empty[String, (Option[Float], Option[String])]
          minMap.put(r.monitor, map)
          map
        })
      monitorMap.put(r.monitorType, (Some(r.value), Some(r.status)))
    }

    Console.println("Total #=" + recordMap.size)
    Console.println("Flush record to new table...")
    try {
      writeSixSecRecord(recordMap, start)
    } catch {
      case e: Exception =>
        Console.print("Ignore update exception...")
    }
  }


  def main(args: Array[String]) {
    if(args.length != 1){
      Console.println("DbConvert [year]")
      Console.println("e.g. DbConvert 2014")
      return
    }

    val year = args(0).toInt
      
    setupDB
    val start = DateTime.parse(s"${year}-1-1")
    var current = start
    
    val end = start + 1.year
    Console.println(s"convert ${start.getYear} Hr record")
    while (current < end) {
      convertHourRecordForOneMonth(current)
      current += 1.month
    }
  /*
    Console.println(s"convert ${start.year} Min record")
    current = start
    while (current < end) {
      convertMinRecordForOneDay(current)
      current += 1.day
    }

  
    Console.println(s"convert ${start.year} 6 sec record")
    current = start
    val end = start + 1.day
    while (current < end) {
      convertSixSecRecordForOneDay(current)
      current += 1.day
    }
    * 
    */
    Console.println("Done")
  }

}