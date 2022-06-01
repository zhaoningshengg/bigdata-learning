package pers.zns.bigdata.learning.csv

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper
import pers.zns.bigdata.learning.common.utils.flink.{EnvUtils, TableUtils}
import pers.zns.bigdata.learning.models.StaticModel
import pers.zns.bigdata.learning.common.utils.constant.PropertiesConstants.{MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USERNAME}

import java.io.File
import scala.collection.JavaConverters._

object Csv2MysqlStaticTxt {
  val propMap: Map[String, String] =
    Map("parallelism.default" -> "1",
      "pipeline.name" -> "Csv2MysqlStaticTxt",
      MYSQL_HOST -> "192.168.30.102",
      MYSQL_PORT -> "3306",
      MYSQL_USERNAME -> "root",
      MYSQL_PASSWORD -> "hitocas",
      "sourceFilePath" -> "/Users/zns/hito/304/0527错误/3/static-test1.txt"
      //      "sourceFilePath" -> "/Users/zns/hito/304/0527错误/3/static_2018-05-01_2018-05-05_20220527142134033.txt.retypec"
    )

  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = EnvUtils.getParameterTool(args, propMap.asJava)
    val tableUtils = new TableUtils(parameterTool)
    val streamEnv = tableUtils.getBlinkStreamEnv


    val mapper = new CsvMapper
    val schema = mapper.schemaFor(classOf[StaticModel]).withoutQuoteChar.withColumnSeparator('~')
    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.formats.csv.CsvReaderFormat
    val csvFormat = CsvReaderFormat.forSchema(mapper, schema, TypeInformation.of(classOf[StaticModel]))
    val source: FileSource[StaticModel] = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(parameterTool.getRequired("sourceFilePath")))).build()

    val modelStream = streamEnv.fromSource(source, WatermarkStrategy.noWatermarks[StaticModel](), "csv-source")
      .map(model => {
        val str = model.getMmsi.replaceFirst("y ", "")
        model.setMmsi(str)
        model
      })

    modelStream.addSink(
      JdbcSink.sink(
        """insert into static_01
          | (id, mmsi, imo, callSign, shipName, shipType, shipLength, shipBreadth, fixingDevice, eta, draft,
          | destination, deviceType, countryCode, receiveTime, sourceId, toBow, toStern, toPort, toStarboard,
          | messageType, baseStationId, mixShipType, dwt, gt, originalType, shipTypeLabel, shipTypeEnLabel)
          | values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin,
        (statement, model) => {
          statement.setInt(1, 0)
          statement.setString(2, model.getMmsi)
          statement.setString(3, model.getImo)
          statement.setString(4, model.getCallSign)
          statement.setString(5, model.getShipName)
          statement.setString(6, model.getShipType)
          statement.setString(7, model.getShipLength)
          statement.setString(8, model.getShipBreadth)
          statement.setString(9, model.getFixingDevice)
          statement.setString(10, model.getEta)
          statement.setString(11, model.getDraft)
          statement.setString(12, model.getDestination)
          statement.setString(13, model.getDeviceType)
          statement.setString(14, model.getCountryCode)
          statement.setString(15, model.getReceiveTime)
          statement.setString(16, model.getSourceId)
          statement.setString(17, model.getToBow)
          statement.setString(18, model.getToStern)
          statement.setString(19, model.getToPort)
          statement.setString(20, model.getToStarboard)
          statement.setString(21, model.getMessageType)
          statement.setString(22, model.getBaseStationId)
          statement.setString(23, model.getMixShipType)
          statement.setString(24, model.getDwt)
          statement.setString(25, model.getGt)
          statement.setString(26, model.getOriginalType)
          statement.setString(27, model.getShipTypeLabel)
          statement.setString(28, model.getShipTypeEnLabel)
        },
        JdbcExecutionOptions.builder()
          .withBatchSize(1000)
          .withBatchIntervalMs(200)
          .withMaxRetries(5)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://" + parameterTool.getRequired(MYSQL_HOST) + ":" + parameterTool.getRequired(MYSQL_PORT) + "/test")
          .withDriverName("com.mysql.cj.jdbc.Driver")
          .withUsername(parameterTool.getRequired(MYSQL_USERNAME))
          .withPassword(parameterTool.getRequired(MYSQL_PASSWORD))
          .build()
      ))

    streamEnv.execute()

  }

}
