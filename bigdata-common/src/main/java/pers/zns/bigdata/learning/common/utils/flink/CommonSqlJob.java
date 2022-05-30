package pers.zns.bigdata.learning.common.utils.flink;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

import static pers.zns.bigdata.learning.common.utils.constant.PropertiesConstants.SQL_FILES_KEY;


/**
 * @program: econ-analysis
 * @description: 普通Sql job通用模板
 * @author: zns
 * @create: 2021-12-13 10:38
 */
@Data
@Builder
@Slf4j
public class CommonSqlJob {
    public String[] args;
    public Map<String, String> propsMap;

    public void executeSqlJob() throws Exception {
        final ParameterTool parameterTool = EnvUtils.getParameterTool(this.args, this.propsMap);
        TableUtils tableUtils = new TableUtils(parameterTool);
        String sqlFilesStr = parameterTool.get(SQL_FILES_KEY);
        log.info("sql文件路径:{}", sqlFilesStr);
        tableUtils.executeSqlFiles(sqlFilesStr);
        tableUtils.executeSqlJob();
    }

}