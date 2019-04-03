import java.io.File

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Starter {

  def main(args: Array[String]): Unit = {

//    val dirName = "/Users/chaitanyawaikar/Desktop/Projects/files"
    val fileNames = getListOfFiles(args(0))

    val tuple = fileProcessor(fileNames)
    val sensorStats = getSensorStats(tuple._2)

    println(s"Num of processed files: ${tuple._1}")
    println(s"Num of processed measurements: ${sensorStats.measurementsProcessed}")
    println(s"Num of failed measurements: ${sensorStats.failedMeasurements}")
    println("Sensors data :")
    println(s"Sensor Name, MinReading, AvgReading, MaxReading")
    sensorStats.sensorStatsList.foreach(x => println(s"Sensor ${x.name}, ${x.min}, ${x.avg}, ${x.max}"))
  }

  private def getSensorStats(sensorData: List[SensorData]): CumulativeResult = {
    val cumulativeSensorStats = sensorData.groupBy(_.name).collect {
      case (k, v) =>
        val sensorReadings = v.filter(!_.reading.toLowerCase.equals("nan"))
        if (sensorReadings.isEmpty)
          SensorStats(k, NaN, NaN, NaN)
        else {
          val avgReading = (sensorReadings.map(f).sum / sensorReadings.size).toString
          val minReading = sensorReadings.minBy(f).reading.toDouble.toString
          val maxReading = sensorReadings.maxBy(f).reading.toDouble.toString
          SensorStats(k, minReading, avgReading, maxReading)
        }
    }.toList
    val measurementsFailed = sensorData.count(_.reading.toLowerCase.equals("nan"))
    val measurementsSuccessful = sensorData.size - measurementsFailed
    CumulativeResult(measurementsSuccessful, measurementsFailed, sortSensorStats(cumulativeSensorStats))
  }

  private def sortSensorStats(listOfSensorStats: List[SensorStats]): List[SensorStats] = {
    val (failedMeasurements, successfulMeasurements) = listOfSensorStats.partition(_.avg.toLowerCase == NaN)
    successfulMeasurements.sortBy(-_.avg.toDouble) ++ failedMeasurements
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(_.getName.contains("csv"))
    } else {
      List[File]()
    }
  }

  private def fileProcessor(files: List[File]): (Int, List[SensorData]) = {
    val fileReaderResponses = files.map(readFromFile)
    val noOfFilesComputed = fileReaderResponses.count(_.isRight)
    val aggregatedRecordsFromAllFiles = fileReaderResponses.filter(_.isRight).flatMap(_.right.get)
    (noOfFilesComputed, aggregatedRecordsFromAllFiles)
  }

  private def readFromFile(file: File): Either[String, List[SensorData]] = {
    Try {
      val listBuffer = ListBuffer[SensorData]()
      val bufferedSource = io.Source.fromFile(file)

      for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        listBuffer += SensorData(cols(0), cols(1))
      }
      bufferedSource.close
      listBuffer.toList match {
        case h :: t => t
        case _ => List()
      }
    } match {
      case scala.util.Success(sensorList) =>
        Right(sensorList)
      case scala.util.Failure(exception) =>
        Left(exception.getMessage)
    }
  }

  private val f: SensorData => Double = _.reading.toDouble

  private val NaN = "NaN"

  case class SensorData(name: String, reading: String)

  case class SensorStats(name: String, min: String, avg: String, max: String)

  case class CumulativeResult(measurementsProcessed: Long, failedMeasurements: Long, sensorStatsList: List[SensorStats])

}
