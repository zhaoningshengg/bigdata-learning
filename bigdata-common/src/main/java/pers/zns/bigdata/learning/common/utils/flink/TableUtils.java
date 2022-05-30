package pers.zns.bigdata.learning.common.utils.flink;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pers.zns.bigdata.learning.common.utils.common.Splitter;
import pers.zns.bigdata.learning.common.utils.constant.PropertiesConstants;
import pers.zns.bigdata.learning.common.utils.file.File2StreamUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @program: util
 * @description:
 * @author: zns
 * @create: 2020-11-06 10:13
 */
@Slf4j
public class TableUtils {
    @Getter
    private final StreamExecutionEnvironment blinkStreamEnv;
    @Getter
    private final TableEnvironment blinkTableEnv;
    private final ParameterTool parameterTool;
    private final Splitter splitter = new Splitter(';');
    private String annotationSymbol = "--";
    private Integer sqlLineCount = 2000;

    public TableUtils(TableEnvironment blinkTableEnv, ParameterTool parameterTool) {
        this.blinkStreamEnv = null;
        this.blinkTableEnv = blinkTableEnv;
        this.parameterTool = parameterTool;
    }

    public TableUtils(TableEnvironment blinkTableEnv, ParameterTool parameterTool, String annotationSymbol) {
        this.blinkStreamEnv = null;
        this.blinkTableEnv = blinkTableEnv;
        this.parameterTool = parameterTool;
        this.annotationSymbol = annotationSymbol;
    }

    public TableUtils(StreamExecutionEnvironment blinkStreamEnv, ParameterTool parameterTool) {
        this.blinkStreamEnv = blinkStreamEnv;
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.addConfiguration(parameterTool.getConfiguration());
        this.blinkTableEnv = StreamTableEnvironment.create(blinkStreamEnv, tableConfig);
        this.parameterTool = parameterTool;
    }

    public TableUtils(StreamExecutionEnvironment blinkStreamEnv, ParameterTool parameterTool, Integer sqlLineCount) {
        this.blinkStreamEnv = blinkStreamEnv;
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.addConfiguration(parameterTool.getConfiguration());
        this.blinkTableEnv = StreamTableEnvironment.create(blinkStreamEnv, tableConfig);
        this.parameterTool = parameterTool;
        this.sqlLineCount = sqlLineCount;
    }

    public TableUtils(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
        this.blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment(parameterTool.getConfiguration());
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.addConfiguration(parameterTool.getConfiguration());
        this.blinkTableEnv = StreamTableEnvironment.create(blinkStreamEnv, tableConfig);
    }


    public String getSqlFromFile(String path) throws Exception {
        String sqlStrings = IOUtils.toString(
                Objects.requireNonNull(File2StreamUtil.getStream(path)),
                StandardCharsets.UTF_8);
        return
                CharSequenceUtil.trim(
//                        去掉空白行
                        ReUtil.replaceAll(
//                                去掉注释行
                                ReUtil.replaceAll(
                                        sqlStrings
//                                              为了匹配最后一行是注释的情况
                                                + "\n"
                                        , this.annotationSymbol + ".*?\\n", ""),
                                "(?m)^\\s*$(\\n|\\r\\n)", "")
                );
    }

    public List<String> getSqlListFromFile(String path) throws Exception {
        return this.getSqlListFromFile(path, this.parameterTool);
    }

    public List<String> getSqlListFromFile(String path, ParameterTool parameterTool) throws Exception {
        if (parameterTool == null) {
            return this.getSqlListFromFile(path, Collections.emptyMap());
        } else {
            return this.getSqlListFromFile(path, parameterTool.toMap());
        }
    }

    public List<String> getSqlListFromFiles(String sqlFilesStr, ParameterTool parameterTool) {
        List<String> sqlFiles = CharSequenceUtil.splitTrim(sqlFilesStr, ",");
        List<String> sqlList = new ArrayList<>();
        sqlFiles.forEach(sqlFile -> {
            if (parameterTool == null) {

                try {
                    sqlList.addAll(this.getSqlListFromFile(sqlFile, Collections.emptyMap()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    sqlList.addAll(this.getSqlListFromFile(sqlFile, parameterTool.toMap()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return sqlList;
    }

    /**
     * @param path   sql文件路径
     * @param params sql中配置的属性
     * @return sql的List
     */
    public List<String> getSqlListFromFile(String path, Map<String, String> params) throws Exception {
        String sqlStr = getSqlFromFile(path);
//        替换配置的属性
        sqlStr = new StringSubstitutor(params).replace(sqlStr);

        return sqlSplit(sqlStr);
    }

    public List<String> sqlSplit(String sqlStr) {
        return splitter.splitEscaped(sqlStr);
    }

    public void executeSqlFile(TableEnvironment blinkTableEnv, ParameterTool parameterTool, String path) throws Exception {
        List<String> sqlList = getSqlListFromFile(path, parameterTool.toMap());
        log.info("\n{}的内容:\n{}\n", path, sqlList);
        sqlList.forEach(blinkTableEnv::sqlUpdate);
    }

    public void executeSqlFile(String path) throws Exception {
        this.executeSqlFile(path, Collections.emptyMap());
    }

    public void executeSqlFileList(List<String> sqlFiles) {
        sqlFiles.forEach(path -> {
            try {
                this.executeSqlFile(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void executeSqlFiles(String sqlFilesStr) throws IOException {
        List<String> sqlFiles = CharSequenceUtil.splitTrim(sqlFilesStr, ",");
        this.executeSqlFileList(sqlFiles);
    }


    public void executeSqlFile(String path, String key, String value) throws Exception {
        this.executeSqlFile(path, MapUtil.of(key, value));
    }

    /**
     * @param path
     * @param preParams 高优先级的参数map
     * @throws IOException
     */
    public void executeSqlFile(String path, Map<String, String> preParams) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.putAll(this.parameterTool.toMap());
        map.putAll(preParams);
        List<String> sqlList = getSqlListFromFile(path, map);
        sqlList.forEach(
                sql ->
                {
                    if (StrUtil.isNotBlank(sql)) {
                        log.info("\n执行{}中的sql:\n{}\n", path, sql);
                        this.blinkTableEnv.sqlUpdate(sql);
//                        this.blinkTableEnv.executeSql(sql);
                    }
                });
//        sqlList.forEach(this.blinkTableEnv::executeSql);
    }

    public void executeSqlStrings(String sqlStr, Map<String, String> preParams) {
        Map<String, String> map = new HashMap<>();
        map.putAll(this.parameterTool.toMap());
        map.putAll(preParams);
        sqlStr = new StringSubstitutor(map).replace(sqlStr);
        List<String> sqlList = sqlSplit(sqlStr);
        sqlList.forEach(
                sql ->
                {
                    if (CharSequenceUtil.isNotBlank(sql)) {
                        log.info("\n执行sql:\n{}\n", sql);
                        this.blinkTableEnv.sqlUpdate(sql);
//                        this.blinkTableEnv.executeSql(sql);
                    }
                });
    }

    public void executeSqlFile(String dir, String fileName) throws Exception {
        this.executeSqlFile(this.combinePath(dir, fileName));

    }

    public void executeSqlFile(TableEnvironment blinkTableEnv, ParameterTool parameterTool, String dir, String fileName) throws Exception {
        this.executeSqlFile(blinkTableEnv, parameterTool, this.combinePath(dir, fileName));
    }

    public List<String> getSqlListFromFile(String dir, String fileName) throws Exception {
        return this.getSqlListFromFile(this.combinePath(dir, fileName));
    }

    public String combinePath(String dir, String fileName) {
        if (CharSequenceUtil.endWith(dir, '/')) {
            return dir + fileName;
        } else {
            return dir + '/' + fileName;
        }
    }

    public void executeSqlJob() throws Exception {
        executeSqlJob("Flink SQL Job");
    }

    public void executeSqlJob(String jobName) throws Exception {
        this.blinkTableEnv.execute(this.parameterTool.get(PropertiesConstants.JOB_NAME, jobName));
    }

    public void executeStreamJob() throws Exception {
        executeStreamJob("Flink Stream Job");
    }

    public void executeStreamJob(String jobName) throws Exception {
        this.blinkStreamEnv.execute(this.parameterTool.get(PropertiesConstants.JOB_NAME, jobName));
    }
}