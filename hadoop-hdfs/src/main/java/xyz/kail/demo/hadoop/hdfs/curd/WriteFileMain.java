package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.IOException;

public class WriteFileMain {

    public static void main(String[] args) throws IOException {
        try (FileSystem fileSystem = HdfsTool.getFileSystem()){

            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/kail/test"));




        }

    }


}
