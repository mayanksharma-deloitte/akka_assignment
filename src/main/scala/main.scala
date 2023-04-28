import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import scalaFunctions.CsvReader.readCsvFile

import scalaFunctions.{CsvReader, MasterActor2, errorLogFlow, financialYearAggregator, processBulkProductInsights, processCategory, processCsv, processCsvFile, processCsvFile1, processErrorRecords}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val categoryFilter = config.getString("categoryFilter")
    val categorySinkFile = config.getString("categorySinkFile")
    val buffer = config.getInt("buffer")
    implicit val system = ActorSystem("ProcessErrorRecordsFlow")
    implicit val materializer = ActorMaterializer()
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val financialYear = config.getInt("FinancialYear")
    val categorywiseFinancialYearSinkFile = config.getString("CategorywiseFinancialYearSinkFile")
    val bulkQuantityValue = config.getInt("BulkQuantityValue")
    val bulkProductInsightSinkFile = config.getString("BulkProductInsightSinkFile")
    val sourceFilename = "src/main/resources/Superstore_purchases.csv"
    val errorLogFile = config.getString("ErrorLogFile")
    val ErrorRecordFileName = config.getString("ErrorRecordFileName")
    val inputFilePath = "src/main/resources/Superstore_purchases.csv"









    val input = scala.io.StdIn.readInt()
    val result = input match {
      case 1 => processCsvFile1(inputFilePath)
      case 2 => readCsvFile(inputFilePath)
      case 3 =>
      case 4 =>
        val masterActor: ActorRef = system.actorOf(Props[MasterActor2], name = "masterActor")
        val futureResults: Future[Unit] = processCsv(inputFilePath,masterActor)
        futureResults.onComplete(_ => {
          println("All records processed")
          system.terminate()
        })

        futureResults.onComplete(_ => {
          println("All records processed")
          system.terminate()
        })

      case 5 =>
        val result = processCategory(categoryFilter, categorySinkFile, buffer)
        // Do something with the result
        result.onComplete { r =>
          println(s"Result: $r")
          system.terminate()
        }
      case 6 =>
        val resultFuture = financialYearAggregator(categorySinkFile, categoryFilter, financialYear, categorywiseFinancialYearSinkFile)
        val result = Await.result(resultFuture, 10.seconds)
        println(s"${result.status}, ${result.count} bytes read")
      case 7 =>
        processBulkProductInsights(bulkQuantityValue, bulkProductInsightSinkFile, sourceFilename, buffer)
      case 8 =>
        processCsvFile("src/main/resources/Superstore_purchases.csv")

      case 9 =>
        val sourceFilename = "src/main/resources/Superstore_purchases.csv"
         errorLogFlow(sourceFilename,errorLogFile)
      case 10 =>
        processErrorRecords(inputFilePath,ErrorRecordFileName)

      case _ => println("Invalid input!")
    }
  }
}

