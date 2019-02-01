package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.FileInputStream;
import java.io.IOException;

public class WriteFileMain {

    private static final String HOME_PATH = "/user/kail";

    private static final String FILE_NAME = "1548654250.tar.gz";

    public static void main(String[] args) throws IOException {


        Path parentPath = new Path(HOME_PATH + "/tf_model/model_price");

        String localFile = "/Users/kail/Desktop/model_price/" + FILE_NAME;

        try (FileSystem fileSystem = HdfsTool.getFileSystem()) {

            /*
             * 本地文件 copy 到 HDFS
             */
            fileSystem.copyFromLocalFile(new Path(localFile), new Path(parentPath, System.currentTimeMillis() + ".tar.gz"));


            /*
             * 创建文件夹
             */
            fileSystem.mkdirs(parentPath);


            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(parentPath, System.currentTimeMillis() + ".tar.gz"))) {

                /*
                 * 读取文件上传
                 */
                try (FileInputStream fileInputStream = new FileInputStream(localFile)) {
                    byte[] buf = new byte[1024 * 1024];

                    for (int index; (index = fileInputStream.read(buf)) > 0; ) {
                        fsDataOutputStream.write(buf, 0, index);
                    }
                }


            }

        }

    }


}
