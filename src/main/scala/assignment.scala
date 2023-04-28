
import akka.actor.AbstractActor.Receive
import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.collection.JavaConverters._
import java.nio.file.{Paths, StandardOpenOption}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.Framing.delimiter
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
case class ProcessRecord(record: Array[String])



class MasterActor extends Actor {
  def receive = {
    case ProcessRecord(record) => {
      println(s"Processing record: ${record.mkString(",")}")
      // process the record here
      // ...
    }
  }
}


/*   2)

class ChildActor extends Actor {
  def receive = {
    case ProcessRecord(record) => {
      println(s"${self.path.name} processing record: ${record.mkString(",")}")
      // process the record here
      // ...
    }
  }
}

class MasterActor extends Actor {
  var router = {
    val routees = Vector.fill(10) {
      val r = context.actorOf(Props[ChildActor])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case ProcessRecord(record) => {
      router.route(ProcessRecord(record), sender())
    }
    case Terminated(a) => {
      router = router.removeRoutee(a)
      val r = context.actorOf(Props[ChildActor])
      context watch r
      router = router.addRoutee(r)
    }
  }
}

*/













/*  4)
class ChildActor extends Actor {
  def receive = {
    case ProcessRecord(record) => {
      println(s"${self.path.name} processing record: ${record.mkString(",")}")
      // process the record here
      // ...
      sender() ! record
    }
  }
}

class MasterActor extends Actor {
  def receive = {
    case ProcessRecord(record) => {
      val childActor = context.actorOf(Props[ChildActor])
      implicit val timeout: Timeout = Timeout(5.seconds)
      val futureResult = (childActor ? ProcessRecord(record)).mapTo[Array[String]]
      sender() ! futureResult
    }
  }
}
object CsvReader {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("CsvReaderSystem")
    val masterActor = system.actorOf(Props[MasterActor], name = "masterActor")

    val filename = "/home/mayanksharma43/Desktop/akka_assignment/src/main/resources/Superstore_purchases.csv"
    val source = Source.fromFile(filename)
    val lines = source.getLines()

    val futureResults = Future {
      for (line <- lines) {
        val record = line.split(",")
        implicit val timeout: Timeout = Timeout(5.seconds)
        val futureResult = (masterActor ? ProcessRecord(record)).mapTo[Array[String]]
        futureResult.onComplete(_ => {
          // do something with the processed record here
          // ...
        })
      }
      source.close()
    }

    futureResults.onComplete(_ => {
      println("All records processed")
      system.terminate()
    })
  }
}
*/



/* 5)

object CategoryFilterFlow {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("CategoryFilterFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val categoryFilter = config.getString("categoryFilter")
    val categorySinkFile = config.getString("categorySinkFile")

    val sourceFilename = "src/main/resources/Superstore_purchases.csv"

    val source = FileIO.fromPath(Paths.get(sourceFilename))

    val sinkFilename = s"src/main/resources/$categorySinkFile"

    val sink = Flow[String].map(_ + "\n").map(ByteString(_)).toMat(FileIO.toPath(Paths.get(sinkFilename)))(Keep.right)

    val filterFlow = Flow[String].filter(line => line.split(",")(9) == categoryFilter)

    val buffer = config.getInt("buffer")
    val stream = source
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 10000, allowTruncation = true))
      .map(_.utf8String)
      .via(filterFlow)
      .buffer(buffer, OverflowStrategy.backpressure)
      .toMat(sink)(Keep.right)

    stream.run().onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }
}




*/




/* 6
object FinancialYearAggregatorFlow {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("FinancialYearAggregatorFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val financialYear = config.getInt("FinancialYear")
    val categoryFilter = config.getString("categoryFilter")
    val categorySinkFile = config.getString("categorySinkFile")
    val categorywiseFinancialYearSinkFile = config.getString("CategorywiseFinancialYearSinkFile")

    val sourceFilename = "src/main/resources/" + categorySinkFile
    val source = FileIO.fromPath(Paths.get(sourceFilename))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)

    val sinkFilename = "src/main/resources/" + categorywiseFinancialYearSinkFile


    val sink = Flow[String].map(_ + "\n").map(ByteString.apply)
      .toMat(FileIO.toPath(Paths.get(sinkFilename)))(Keep.right)

    val filterFlow = Flow[String].filter(line => {
      val fields = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      val dateStr = fields(0)
      val year = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("M/d/yyyy")).getYear()

      fields(9) == categoryFilter && year == financialYear
    })


    val sumFlow = Flow[String].fold((0.0, 0.0, 0.0, 0.0)) {
      case ((s1, s2, s3, s4), line) =>
        val fields = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
        val sales = fields(12).toDouble
        val quantity = fields(13).toDouble
        val discount = fields(14).toDouble
        val profit = fields(15).toDouble
        (s1 + sales, s2 + quantity, s3 + discount, s4 + profit)
    }

    val buffer = config.getInt("buffer")
    val stream = source.via(filterFlow).via(sumFlow).map {
      case (sales, quantity, discount, profit) =>
        s"$categoryFilter,$financialYear,$sales,$quantity,$discount,$profit"
    }.buffer(buffer, OverflowStrategy.backpressure).toMat(sink)(Keep.right)

    stream.run().onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }
}

*/





/* 7)
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global

object BulkProductInsightsFlow {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("BulkProductInsightsFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val bulkQuantityValue = config.getInt("BulkQuantityValue")
    val bulkProductInsightSinkFile = config.getString("BulkProductInsightSinkFile")
    val headers = "FinancialYear,TotalProfit,TotalDiscount,AvgProfit,AvgDiscount,TotalQuantitySales,AvgQuantitySales,TotalSales,AvgSales"

    val sourceFilename = "src/main/resources/Superstore_purchases.csv"
    val source = FileIO.fromPath(Paths.get(sourceFilename))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)).drop(1)
      .map(_.utf8String)

    val sinkFilename = "src/main/resources/" + bulkProductInsightSinkFile
    val sink = Flow[String].map(_ + "\n").map(ByteString.apply)
      .toMat(FileIO.toPath(Paths.get(sinkFilename)))(Keep.right)

    val filterFlow = Flow[String].filter(line => {
      val fields = line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      if (fields.length < 16) {
        false
      } else {
        val dateStr = fields(0)
        val year = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("M/d/yyyy")).getYear()
        val quantity = fields(13).toDouble
        quantity >= bulkQuantityValue && year == 2017 // hardcoding year as 2017 for simplicity
      }
    })


    val metricsFlow = Flow[String].fold((0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)) {
      case ((tp, td, ap, ad, tq, aq, ts, as), line) =>
        val fields = line.replaceAll("\\r", "").split(",(?=(?:[^\\\\\\\"]*\\\\\\\"[^\\\\\\\"]*\\\\\\\")*[^\\\\\\\"]*$)")
        if (fields.length == 16) {
          val profit = fields(15).toDouble
          val discount = fields(14).toDouble
          val quantity = fields(13).toDouble
          val sales = fields(12).toDouble
          (tp + profit, td + discount, ap + profit / quantity, ad + discount / quantity,
            tq + quantity, aq + quantity / 1, ts + sales, as + sales / quantity)
        } else {
          println(s"Ignoring record with invalid number of columns: $line")
          (tp, td, ap, ad, tq, aq, ts, as)
        }
    }


    //    val headers = "FinancialYear,TotalProfit,TotalDiscount,AvgProfit,AvgDiscount,TotalQuantitySales,AvgQuantitySales,TotalSales,AvgSales"
   val buffer = config.getInt("buffer")

    val stream = source.via(filterFlow).via(metricsFlow).map {
      case (tp,td,ap,ad,tq,aq,ts,as) =>
        s"2017,$tp,$td,$ap,$ad,$tq,$aq,$ts,$as"
    }.buffer(buffer, OverflowStrategy.backpressure).toMat(sink)(Keep.right)


    stream.run().onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }
}
*/



/* 8)

case class ValidationFailureRecord(failureRecord: String, errorMessage: String, columnName: String)

object ValidationFailureActor {
  def props: Props = Props[ValidationFailureActor]
}

class ValidationFailureActor extends Actor {
  def receive: Receive = {
    case record: ValidationFailureRecord =>
      println(s"Validation failure: $record")
  }
}

object CsvReader {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("CsvReader")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val filename = "src/main/resources/Superstore_purchases.csv"

    val validationFailureActor = system.actorOf(ValidationFailureActor.props)

    val validateField = (value: String, columnName: String) => {
      if (value.trim.isEmpty) {
        val errorMessage = s"Empty value in column '$columnName'"
        val failureRecord = s"$value|$errorMessage|$columnName"
        validationFailureActor ! ValidationFailureRecord(failureRecord, errorMessage, columnName)
        "NULL"
      } else {
        value
      }
    }

    val validateLine = (line: String) => {
      val fields = line.split(",", -1)
      if (fields.length != 16) {
        val errorMessage = s"Invalid number of columns: ${fields.length}"
        val failureRecord = s"$line|$errorMessage|"
        validationFailureActor ! ValidationFailureRecord(failureRecord, errorMessage, "")
        None
      } else {
        Some(fields.map(field => validateField(field, fields.indexOf(field).toString)).mkString(","))
      }
    }

    val source = FileIO.fromPath(Paths.get(filename))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)
      .drop(1)
      .map(line => validateLine(line))
      .collect { case Some(line) => line }

    val sink = Sink.foreach[String](println)

    val stream = source.toMat(sink)(Keep.right)

    stream.run().onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }
}
*/



/* 9)

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.nio.file.Paths
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

case class ValidationError(columnName: String, errorMsg: String)

object ErrorLogFlow {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("ErrorLogFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val errorLogFile = config.getString("ErrorLogFile")

    val sourceFilename = "src/main/resources/Superstore_purchases.csv"
    val source = FileIO.fromPath(Paths.get(sourceFilename))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)).drop(1)
      .map(_.utf8String)

    val errorLogFilename = "src/main/resources/" + errorLogFile.replace("{todaysdate}", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    val sink = Flow[ValidationError].map(v => s"${v.columnName},${v.errorMsg}\n").map(ByteString.apply)
      .toMat(FileIO.toPath(Paths.get(errorLogFilename)))(Keep.right)

    val filterFlow = Flow[String].filter(line => {
      val fields = line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      fields.length == 16
    })

    val validateFlow = Flow[String].map(line => {
      val fields = line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
      if (fields(13).toDouble < 0) {
        Some(ValidationError("Quantity", "Quantity cannot be negative"))
      } else if (fields(15).toDouble < 0) {
        Some(ValidationError("Profit", "Profit cannot be negative"))
      } else {
        None
      }
    }).collect { case Some(v) => v }

    val errorLogFlow = Flow[ValidationError].to(sink)

    val stream = source.via(filterFlow).via(validateFlow)
      .alsoTo(errorLogFlow)
      .runWith(Sink.ignore)

    stream.onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }
}
*/


/* 10)


import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.concurrent.Future

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("ErrorRecords")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val ErrorRecordFileName = s"Error_records_${LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}.csv"
    val inputFilePath = "src/main/resources/Superstore_purchases.csv"

    val inputSource = FileIO.fromPath(Paths.get(inputFilePath))
    val outputSink = FileIO.toPath(Paths.get(ErrorRecordFileName), options = Set(StandardOpenOption.CREATE, StandardOpenOption.APPEND))

    val successfulRecords = inputSource
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
      .map(_.utf8String)
      .drop(1) // skip header
      .map(line => line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim))
      .filter(fields => fields.length == 16 && fields(0).nonEmpty && fields(1).nonEmpty && fields(15).nonEmpty) // example validation

    val failedRecords: Future[IOResult] = inputSource
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
      .map(_.utf8String)
      .drop(1) // skip header
      .map(line =>line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim))
      .filterNot(fields => fields.length == 16 && fields(0).nonEmpty && fields(1).nonEmpty && fields(15).nonEmpty) // example validation
      .map(fields => ByteString(fields.mkString(",") + "\n"))
      .runWith(outputSink)

    successfulRecords.runWith(Sink.ignore)
      .onComplete(_ => {
        failedRecords.onComplete(_ => system.terminate())
      })
  }
}
*/














































object assignment {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("CsvReaderSystem")
    val masterActor = system.actorOf(Props[MasterActor], name = "masterActor")

    val filename = "/home/mayanksharma43/Desktop/akka_assignment/src/main/resources/Superstore_purchases.csv"
    val source = Source.fromFile(filename)
    val lines = source.getLines()

    val futureResults = Future {
      for (line <- lines) {
        val record = line.split(",")
        masterActor ! ProcessRecord(record)
      }
      source.close()
    }

    futureResults.onComplete(_ => {
      println("All records processed")
      system.terminate()
    })
  }

}
