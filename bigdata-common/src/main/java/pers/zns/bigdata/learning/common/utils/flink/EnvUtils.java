package pers.zns.bigdata.learning.common.utils.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.zns.bigdata.learning.common.utils.constant.PropertiesConstants;
import pers.zns.bigdata.learning.common.utils.file.File2StreamUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * @program: util
 * @description:
 * @author: zns
 * @create: 2020-11-13 11:29
 */
@Slf4j
public class EnvUtils {
    private EnvUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static ParameterTool getParameterTool(String[] args) throws IOException {
        return getParameterTool(args, Collections.emptyMap());
    }

    public static ParameterTool getParameterTool(String[] args, Map<String, String> propsMap) throws IOException {
        //1.读取参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        //2.根据参数进行配置文件的选择，没有则默认 DEFAULT_ACTIVE
        ParameterTool activeTool = ParameterTool.fromPropertiesFile(File2StreamUtil.getStream(PropertiesConstants.PROPERTIES_FILE_NAME));
        String active = fromArgs.get(PropertiesConstants.ACTIVE,
                activeTool.get(PropertiesConstants.ACTIVE, PropertiesConstants.DEFAULT_ACTIVE));
        //3.读取配置文件
        ParameterTool fromPropertiesFile = ParameterTool.fromPropertiesFile(File2StreamUtil.getStream(active));
        //4.参数覆盖重复项,优先级从低到高为:application.properties->dev.properties->propsMap->args
        ParameterTool parameterTool = activeTool
                .mergeWith(fromPropertiesFile)
                .mergeWith(ParameterTool.fromMap(propsMap))
                .mergeWith(fromArgs);
        //log format
        StringBuffer stringBuffer = new StringBuffer();
        TreeMap<String, String> treeMap = new TreeMap<>(parameterTool.toMap());
        treeMap.forEach((key, value) ->
                stringBuffer.append(key).append(":").append(value).append("\n"));
        log.info("读取到的参数:\n{}", stringBuffer);
        return parameterTool;
    }

    public static StreamExecutionEnvironment getStreamEnv(String[] args) throws IOException {
        return getStreamEnv(args, Collections.emptyMap());
    }

    public static StreamExecutionEnvironment getStreamEnv(String[] args, Map<String, String> propsMap) throws IOException {
        return StreamExecutionEnvironment.getExecutionEnvironment(getParameterTool(args, propsMap).getConfiguration());
    }

}