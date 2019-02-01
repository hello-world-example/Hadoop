package xyz.kail.demo.hadoop.hdfs.gzip;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 归档
 */
public class TarUtilsTest {

    private static final String HOME_PATH = "/Users/kail/Desktop/model_price";

    @Test
    public void archive() throws IOException, CompressorException {
        TarUtil.archive(HOME_PATH + "/1548414241");

        CompressorOutputStream gzippedOut = new CompressorStreamFactory()
                .createCompressorOutputStream(CompressorStreamFactory.GZIP, new FileOutputStream(HOME_PATH + "/test/1548414241.tar.gz"));

        InputStream tarIn = Files.newInputStream(Paths.get(HOME_PATH + "/1548414241.tar"));

        IOUtils.copy(tarIn, gzippedOut);

        tarIn.close();
        gzippedOut.close();


//        InputStream tarIn = Files.newInputStream(Paths.get(HOME_PATH + "/1548414241.tar"));
//
//        GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(HOME_PATH + "/test/archive.tar.gz"))));
//        final byte[] buffer = new byte[1024];
//        int n = 0;
//        while (-1 != (n = tarIn.read(buffer))) {
//            gzOut.write(buffer, 0, n);
//        }
//        gzOut.close();
//        tarIn.close();

    }

    @Test
    public void dearchive() throws IOException {
        TarUtil.dearchive(HOME_PATH + "/1548414241.2.tar");
    }

}
