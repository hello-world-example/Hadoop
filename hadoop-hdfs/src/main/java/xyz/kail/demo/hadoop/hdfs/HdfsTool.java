package xyz.kail.demo.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.function.Consumer;

public class HdfsTool {

//    public static final String FS_DEFAULT_FS = "hdfs://localhost:8020";
    public static final String FS_DEFAULT_FS = "hdfs://localhost:9000";

    public static final String HADOOP_USER_NAME = "hadoop";

    public static FileSystem getFileSystem() {
        return Fs.getFileSystem();
    }

    private static class Fs {

        private static final Configuration conf = new Configuration();

        static {
            conf.set("fs.defaultFS", FS_DEFAULT_FS);
            //更改操作用户为hadoop
            System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);
        }

        private static FileSystem getFileSystem() {
            try {
                return FileSystem.get(conf);
            } catch (IOException e) {
                throw new Error("无法获取 HDFS 文件系统", e);
            }
        }

    }


}
