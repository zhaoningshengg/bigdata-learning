package pers.zns.bigdata.learning.txt;

import cn.hutool.core.map.MapUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.zns.bigdata.learning.common.utils.flink.EnvUtils;
import pers.zns.bigdata.learning.common.utils.flink.TableUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: bigdata-learning
 * @description: txt内容去重
 * @author: zns
 * @create: 2022-06-09 16:04
 */
public class TxtDataDedup {
    static final Map<String, String> APP_PROPS = MapUtil.builder(new HashMap<String, String>())
            .put("execution.runtime-mode", "BATCH")
            .put("parallelism.default", "1")
            .put("pipeline.name", "TxtDataDedup")
            .put("sourceFilePath", "bigdata-flink-learning/src/main/resources/data/indicator_meta.txt")
            .build();

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = EnvUtils.getParameterTool(args, APP_PROPS);
        TableUtils tableUtils = new TableUtils(parameterTool);
        StreamExecutionEnvironment blinkStreamEnv = tableUtils.getBlinkStreamEnv();
        blinkStreamEnv.readTextFile(parameterTool.getRequired("sourceFilePath"))
                .keyBy(line -> line)
                .reduce((a, b) -> b)
                .print(">>>>>");
        blinkStreamEnv.execute();
    }
}