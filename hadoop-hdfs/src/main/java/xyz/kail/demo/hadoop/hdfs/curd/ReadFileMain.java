package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * https://blog.csdn.net/JENREY/article/details/79761414
 *
 * https://blog.csdn.net/ka_ka314/article/details/83059006
 */
public class ReadFileMain {

    private static final String HOME_PATH = "/user/kail";

    public static void main(String[] args) throws IOException {


        Path remarkFile = new Path(HOME_PATH + "/remark_200.txt");

        ByteBuffer byteBuffer = ByteBuffer.allocate(10 * 1024);

        try (FileSystem fileSystem = HdfsTool.getFileSystem()) {
            FSDataInputStream fsDataInputStream = fileSystem.open(remarkFile);


            int writeIndex = fsDataInputStream.read(byteBuffer);

        }


    }


}
