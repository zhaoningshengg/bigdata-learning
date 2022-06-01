package pers.zns.bigdata.learning.csv;

import cn.hutool.core.map.MapUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.zns.bigdata.learning.common.utils.flink.EnvUtils;
import pers.zns.bigdata.learning.common.utils.flink.TableUtils;
import pers.zns.bigdata.learning.models.StaticModel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static pers.zns.bigdata.learning.common.utils.constant.PropertiesConstants.*;

/**
 * @program: bigdata-learning
 * @description:
 * @author: zns
 * @create: 2022-05-31 17:33
 */
public class Csv2Mysql304StaticTxt {
    static final Map<String, String> APP_PROPS = MapUtil.builder(new HashMap<String, String>())
            .put("pipeline.name", "Csv2MysqlStaticTxt")
            .put(MYSQL_HOST, "192.168.30.102")
            .put(MYSQL_PORT, "3306")
            .put(MYSQL_DATABASE, "test")
            .put(MYSQL_TABLE, "static_01")
            .put(MYSQL_USERNAME, "root")
            .put(MYSQL_PASSWORD, "hitocas")
            .put("sourceFilePath", "/Users/zns/hito/304/0527错误/3/static-test1.txt")
//            .put("sourceFilePath", "/Users/zns/hito/304/0527错误/3/static_2018-05-01_2018-05-05_20220527142134033.txt.retypec")
            .build();

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = EnvUtils.getParameterTool(args, APP_PROPS);
        TableUtils tableUtils = new TableUtils(parameterTool);
        StreamExecutionEnvironment blinkStreamEnv = tableUtils.getBlinkStreamEnv();
        //csv写入,使用批模式
        blinkStreamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema csvSchema = csvMapper.schemaFor(StaticModel.class)
                .withoutQuoteChar()
                .withNullValue("")
                .withColumnSeparator('~');
        CsvReaderFormat<StaticModel> csvReaderFormat = CsvReaderFormat.forSchema(csvMapper, csvSchema, TypeInformation.of(StaticModel.class));
        FileSource<StaticModel> source = FileSource.forRecordStreamFormat(csvReaderFormat, Path.fromLocalFile(new File(parameterTool.getRequired("sourceFilePath")))).build();


        SingleOutputStreamOperator<StaticModel> modelStream = blinkStreamEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source")
                .map(model -> {
                    model.setMmsi(model.getMmsi().replaceFirst("y ", ""));
                    return model;
                });

        modelStream.addSink(
                JdbcSink.sink(
                        "insert into " + parameterTool.getRequired(MYSQL_TABLE) + "(id, mmsi, imo, callSign, shipName, shipType, shipLength, shipBreadth, fixingDevice, eta, draft,\n" +
                                "                      destination, deviceType, countryCode, receiveTime, sourceId, toBow, toStern, toPort, toStarboard,\n" +
                                "                      messageType, baseStationId, mixShipType, dwt, gt, originalType, shipTypeLabel, shipTypeEnLabel)\n" +
                                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        ,
                        (statement, model) -> {
                            statement.setInt(1, 0);
                            statement.setString(2, model.getMmsi());
                            statement.setString(3, model.getImo());
                            statement.setString(4, model.getCallSign());
                            statement.setString(5, model.getShipName());
                            statement.setString(6, model.getShipType());
                            statement.setString(7, model.getShipLength());
                            statement.setString(8, model.getShipBreadth());
                            statement.setString(9, model.getFixingDevice());
                            statement.setString(10, model.getEta());
                            statement.setString(11, model.getDraft());
                            statement.setString(12, model.getDestination());
                            statement.setString(13, model.getDeviceType());
                            statement.setString(14, model.getCountryCode());
                            statement.setString(15, model.getReceiveTime());
                            statement.setString(16, model.getSourceId());
                            statement.setString(17, model.getToBow());
                            statement.setString(18, model.getToStern());
                            statement.setString(19, model.getToPort());
                            statement.setString(20, model.getToStarboard());
                            statement.setString(21, model.getMessageType());
                            statement.setString(22, model.getBaseStationId());
                            statement.setString(23, model.getMixShipType());
                            statement.setString(24, model.getDwt());
                            statement.setString(25, model.getGt());
                            statement.setString(26, model.getOriginalType());
                            statement.setString(27, model.getShipTypeLabel());
                            statement.setString(28, model.getShipTypeEnLabel());
                        }
                        , JdbcExecutionOptions.builder()
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://" + parameterTool.getRequired(MYSQL_HOST) + ":" + parameterTool.getRequired(MYSQL_PORT) + "/" + parameterTool.getRequired(MYSQL_DATABASE))
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(parameterTool.getRequired(MYSQL_USERNAME))
                                .withPassword(parameterTool.getRequired(MYSQL_PASSWORD))
                                .build())
        );
        blinkStreamEnv.execute();

    }
}