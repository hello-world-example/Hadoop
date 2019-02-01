package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.IOException;

/**
 * https://blog.csdn.net/JENREY/article/details/79761414
 * <p>
 * https://blog.csdn.net/ka_ka314/article/details/83059006
 */
public class ReadFileMain {

    private static final String HOME_PATH = "/user/kail";

    public static void main(String[] args) throws IOException {


        Path remarkFile = new Path(HOME_PATH + "/remark_200.txt");

        Path localFile = new Path("/tmp/remark_200.txt");

        try (FileSystem fileSystem = HdfsTool.getFileSystem()) {

            fileSystem.copyToLocalFile(remarkFile, localFile);

        }


    }


}
