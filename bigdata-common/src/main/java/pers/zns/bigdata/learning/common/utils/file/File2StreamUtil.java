package pers.zns.bigdata.learning.common.utils.file;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.text.CharSequenceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @program: econ-analysis
 * @description:
 * @author: zns
 * @create: 2022-05-18 11:32
 */
@Slf4j
public class File2StreamUtil {
    public static final String LOCAL_FILE_PREFIX = "file:///";
    public static final String HDFS_FILE_PREFIX = "hdfs://";

    static {
        try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable t) {
            if (!t.getMessage().contains("factory already defined")) {
                log.info(t.getMessage());
            }
        }
    }

    private File2StreamUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static InputStream getStream(String path) throws IOException {
        Assert.notBlank(path, "Path must not be null");
        if (CharSequenceUtil.startWith(path, LOCAL_FILE_PREFIX) || CharSequenceUtil.startWith(path, HDFS_FILE_PREFIX)) {
            return new URL(path).openStream();
        } else {
            Class<?> aClass = deduceMainApplicationClass();
            assert aClass != null;
            return aClass.getResourceAsStream(path);
        }

    }

    public static Class<?> deduceMainApplicationClass() {
        try {
            StackTraceElement[] stackTrace = new RuntimeException().getStackTrace();
            for (StackTraceElement stackTraceElement : stackTrace) {
                if ("main".equals(stackTraceElement.getMethodName())) {
                    return Class.forName(stackTraceElement.getClassName());
                }
            }
        } catch (ClassNotFoundException ex) {
            // Swallow and continue
        }
        return null;
    }
}