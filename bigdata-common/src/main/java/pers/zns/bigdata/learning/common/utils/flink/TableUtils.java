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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
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
    @Getter
    private final StatementSet statementSet;

    private final ParameterTool parameterTool;
    @Getter
    private final Class<?> mainClass;
    private final Splitter splitter;
    private final String annotationSymbol;

    private static final char DEFAULT_DELIMITER = ';';
    private static final String DEFAULT_ANNOTATION_SYMBOL = "--";

    public TableUtils(ParameterTool parameterTool, Class<?> mainClass, char delimiter, String annotationSymbol) {
        this.parameterTool = parameterTool;
        this.mainClass = mainClass;
        this.blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment(parameterTool.getConfiguration());
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.addConfiguration(parameterTool.getConfiguration());
        this.blinkTableEnv = StreamTableEnvironment.create(blinkStreamEnv, EnvironmentSettings.newInstance().withConfiguration(tableConfig.getConfiguration()).build());
        this.statementSet = this.blinkTableEnv.createStatementSet();
        this.splitter = new Splitter(delimiter);
        this.annotationSymbol = annotationSymbol;
    }

    public TableUtils(ParameterTool parameterTool, Class<?> mainClass, char delimiter) {
        this(parameterTool, mainClass, delimiter, DEFAULT_ANNOTATION_SYMBOL);
    }

    public TableUtils(ParameterTool parameterTool, Class<?> mainClass, String annotationSymbol) {
        this(parameterTool, mainClass, DEFAULT_DELIMITER, annotationSymbol);
    }

    public TableUtils(ParameterTool parameterTool, Class<?> mainClass) {
        this(parameterTool, mainClass, DEFAULT_DELIMITER, DEFAULT_ANNOTATION_SYMBOL);
    }

    public TableUtils(ParameterTool parameterTool) {
        this(parameterTool, File2StreamUtil.deduceMainApplicationClass());
    }

    public String getSqlFromFile(String path) throws Exception {
        String sqlStrings = IOUtils.toString(
                Objects.requireNonNull(File2StreamUtil.getStream(path, this.mainClass)),
                StandardCharsets.UTF_8);
        return
                CharSequenceUtil.trim(
//                        ???????????????
                        ReUtil.replaceAll(
//                                ???????????????
                                ReUtil.replaceAll(
                                        sqlStrings
//                                              ??????????????????????????????????????????
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
     * @param path   sql????????????
     * @param params sql??????????????????
     * @return sql???List
     */
    public List<String> getSqlListFromFile(String path, Map<String, String> params) throws Exception {
        String sqlStr = getSqlFromFile(path);
//        ?????????????????????
        sqlStr = new StringSubstitutor(params).replace(sqlStr);

        return sqlSplit(sqlStr);
    }

    public List<String> sqlSplit(String sqlStr) {
        return splitter.splitEscaped(sqlStr);
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
     * @param preParams ?????????????????????map
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
                        log.info("\n??????{}??????sql:\n{}\n", path, sql);
                        this.statementSet.addInsertSql(sql);
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
                        log.info("\n??????sql:\n{}\n", sql);
                        this.statementSet.addInsertSql(sql);
//                        this.blinkTableEnv.executeSql(sql);
                    }
                });
    }

    public void executeSqlFile(String dir, String fileName) throws Exception {
        this.executeSqlFile(this.combinePath(dir, fileName));

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
//        this.blinkTableEnv.execute(this.parameterTool.get(PropertiesConstants.JOB_NAME, jobName));
        this.statementSet.execute();
    }

    public void executeStreamJob() throws Exception {
        executeStreamJob("Flink Stream Job");
    }

    public void executeStreamJob(String jobName) throws Exception {
        this.blinkStreamEnv.execute(this.parameterTool.get(PropertiesConstants.JOB_NAME, jobName));
    }
}