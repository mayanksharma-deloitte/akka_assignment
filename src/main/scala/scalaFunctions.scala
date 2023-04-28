import akka.Done
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import java.nio.file.{Paths, StandardOpenOption}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.{ExecutionContext, Future}

object scalaFunctions {

  // define an implicit execution context
























  // 5)
  def processCategory(categoryFilter: String, categorySinkFile: String, buffer: Int = 15)
                     (implicit system: ActorSystem, materializer: ActorMaterializer,ec: ExecutionContext): Future[Long] = {

    val sourceFilename = "src/main/resources/Superstore_purchases.csv"
    val source = FileIO.fromPath(Paths.get(sourceFilename))

    val sinkFilename = s"src/main/resources/$categorySinkFile"
    val sink = Flow[String].map(_ + "\n").map(ByteString(_)).toMat(FileIO.toPath(Paths.get(sinkFilename)))(Keep.right)

    val filterFlow = Flow[String].filter(line => line.split(",")(9) == categoryFilter)

    val stream = source
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 10000, allowTruncation = true))
      .map(_.utf8String)
      .via(filterFlow)
      .buffer(buffer, akka.stream.OverflowStrategy.backpressure)
      .toMat(sink)(Keep.right)

    stream.run().map(_.count)
  }



 // 6 )
  def financialYearAggregator(categorySinkFile: String, categoryFilter: String, financialYear: Int, categorywiseFinancialYearSinkFile: String) (implicit system: ActorSystem, materializer: ActorMaterializer,ec: ExecutionContext): Future[IOResult] = {
    implicit val system = ActorSystem("FinancialYearAggregatorFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()

    val sourceFilename = s"src/main/resources/$categorySinkFile"
    val sinkFilename = s"src/main/resources/$categorywiseFinancialYearSinkFile"

    val source = FileIO.fromPath(Paths.get(sourceFilename))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true))
      .map(_.utf8String)

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

    val result = stream.run()

    result.onComplete(_ => system.terminate())

    result
  }


/*
  // 7)

  def processBulkProductInsights(bulkQuantityValue: Int, bulkProductInsightSinkFile: String, sourceFilename: String, buffer: Int): Unit = {

    implicit val system = ActorSystem("BulkProductInsightsFlow")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()
    val headers = "FinancialYear,TotalProfit,TotalDiscount,AvgProfit,AvgDiscount,TotalQuantitySales,AvgQuantitySales,TotalSales,AvgSales"

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

    val stream = source.via(filterFlow).via(metricsFlow).map {
      case (tp, td, ap, ad, tq, aq, ts, as) =>
        s"2017,$tp,$td,$ap,$ad,$tq,$aq,$ts,$as"
    }.buffer(buffer, OverflowStrategy.backpressure).toMat(sink)(Keep.right)

    stream.run().onComplete { result =>
      println(s"All records processed: $result")
      system.terminate()
    }
  }


  // 8 )

  case class ValidationFailureRecord(failureRecord: String, errorMessage: String, columnName: String)

  class ValidationFailureActor extends Actor {
    def receive: Receive = {
      case record: ValidationFailureRecord =>
        println(s"Validation failure: $record")
    }
  }

  def processCsvFile(filename: String): Unit = {
    implicit val system = ActorSystem("CsvReader")
    implicit val materializer = ActorMaterializer()

    val config = ConfigFactory.load()

    val validationFailureActor = system.actorOf(Props[ValidationFailureActor])

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


// 9)

  case class ValidationError(columnName: String, errorMsg: String)

  def errorLogFlow(sourceFilename: String, errorLogFile: String)(implicit system: ActorSystem, materializer: ActorMaterializer): Future[Done] = {
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

    stream
  }


/*
// 10)

  def processErrorRecords(inputFilePath: String, errorRecordFileName: String)(implicit system: ActorSystem, materializer: ActorMaterializer, ec: ExecutionContext): Future[Unit] = {
    val inputSource = FileIO.fromPath(Paths.get(inputFilePath))
    val outputSink = FileIO.toPath(Paths.get(errorRecordFileName), options = Set(StandardOpenOption.CREATE, StandardOpenOption.APPEND))

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
      .map(line => line.replaceAll("\\r", "").split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim))
      .filterNot(fields => fields.length == 16 && fields(0).nonEmpty && fields(1).nonEmpty && fields(15).nonEmpty) // example validation
      .map(fields => ByteString(fields.mkString(",") + "\n"))
      .runWith(outputSink)

    successfulRecords.runWith(Sink.ignore)
      .flatMap(_ => {
        failedRecords.map(_ => ())
      })
  }

*/



*/

}
