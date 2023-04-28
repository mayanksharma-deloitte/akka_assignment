import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import scalaFunctions.{financialYearAggregator, processCategory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

object Main {
  def main(args: Array[String]): Unit = {

    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val input = scala.io.StdIn.readInt()
    val result = input match {
      case 5 => implicit val system = ActorSystem("ProcessErrorRecordsFlow")
        implicit val materializer = ActorMaterializer()
        val config = ConfigFactory.load()
        val categoryFilter = config.getString("categoryFilter")
        val categorySinkFile = config.getString("categorySinkFile")
        val buffer = config.getInt("buffer")
        val result = processCategory(categoryFilter, categorySinkFile, buffer)

        // Do something with the result
        result.onComplete { r =>
          println(s"Result: $r")
          system.terminate()
        }
      case 6 => implicit val system = ActorSystem("Main")
        implicit val materializer = ActorMaterializer()
        implicit val ec = ExecutionContext.global

        val config = ConfigFactory.load()
        val financialYear = config.getInt("FinancialYear")
        val categoryFilter = config.getString("categoryFilter")
        val categorySinkFile = config.getString("categorySinkFile")
        val categorywiseFinancialYearSinkFile = config.getString("CategorywiseFinancialYearSinkFile")
        val resultFuture = financialYearAggregator(categorySinkFile, categoryFilter, financialYear, categorywiseFinancialYearSinkFile)
        val result = Await.result(resultFuture, 10.seconds)
        println(s"${result.status}, ${result.count} bytes read")
      case 7 =>
      case _ => println("Invalid input!")
    }
  }
}

