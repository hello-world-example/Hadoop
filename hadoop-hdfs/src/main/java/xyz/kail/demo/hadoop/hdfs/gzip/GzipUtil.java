package xyz.kail.demo.hadoop.hdfs.gzip;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GzipUtil {

    /**
     * 压缩
     *
     * @param sourceFile 源文件 或 路径
     * @param tarGzFile  *.tar.gz
     */
    public static void zcf(String sourceFile, String tarGzFile) throws IOException {
        // 归档
        TarUtil.archive(sourceFile, tarGzFile + ".tar.temp");

        // 压缩
        compress(Paths.get(tarGzFile + ".tar.temp"), Paths.get(tarGzFile), true);
    }


    /**
     * 解压缩
     *
     * @param tarGzFile *.tar.gz
     * @param targetDir 目标 路径
     */
    public static void zxf(String tarGzFile, String targetDir) throws IOException {
        Path tarGzFilePath = Paths.get(tarGzFile);
        Path tempTarPath = Paths.get(tarGzFilePath.getParent().toString(), tarGzFilePath.getFileName() + ".tar.temp");


        // 解压
        decompression(tarGzFilePath, tempTarPath, false);

        // 拆包
        TarUtil.dearchive(tempTarPath.toFile(), targetDir);

        // 删除临时文件
        Files.deleteIfExists(tempTarPath);

    }


    /* ************************************************************************************************************
     * 压缩
     * ************************************************************************************************************/

    /**
     * 压缩 GzipCompressorOutputStream
     */
    public static void compress(Path tarPath, Path tarGzPath, boolean delTar) throws IOException {

        try (
                // 待压缩的文件
                InputStream in = Files.newInputStream(tarPath);

                // 压缩后的文件
                OutputStream fout = Files.newOutputStream(tarGzPath);
                BufferedOutputStream out = new BufferedOutputStream(fout);
        ) {
            compress(in, out);

            if (delTar) {
                Files.deleteIfExists(tarPath);
            }
        }


    }

    public static void compress(InputStream tarInputStream, OutputStream tarGzOutputStream) throws IOException {

        try (
                GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(tarGzOutputStream);
        ) {
            IOUtils.copy(tarInputStream, gzOut);
        }

    }

    /* ************************************************************************************************************
     * 解压缩
     * ************************************************************************************************************/

    // TODO tar.gz

    /**
     * 解压缩 GzipCompressorInputStream
     */
    public static void decompression(Path tarGzPath, Path tarPath, boolean delTarGz) throws IOException {

        try (
                // 解压缩
                InputStream fin = Files.newInputStream(tarGzPath);
                BufferedInputStream in = new BufferedInputStream(fin);

                // 解压后的归档文件
                OutputStream out = Files.newOutputStream(tarPath);
        ) {
            decompression(in, out);

            if (delTarGz) {
                Files.deleteIfExists(tarGzPath);
            }
        }

    }

    public static void decompression(InputStream tarGzInputStream, OutputStream tarOutputStream) throws IOException {

        try (
                // 解压缩
                GzipCompressorInputStream gzIn = new GzipCompressorInputStream(tarGzInputStream);
        ) {
            IOUtils.copy(gzIn, tarOutputStream);
        }

    }

}
