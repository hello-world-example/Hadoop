package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.*;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.IOException;

public class LsMain {

    public static void main(String[] args) throws IOException {
        try (FileSystem fileSystem = HdfsTool.getFileSystem()) {

            /*
             * 只能列出文件，不能列出文件夹
             */
            System.out.println("只能列出文件，不能列出文件夹");
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), false);
            for (; listFiles.hasNext(); ) {
                LocatedFileStatus next = listFiles.next();
                System.out.println(next.getPath().toString());
            }
            System.out.println();

            /*
             * 列出文件和文件夹
             */
            System.out.println("列出文件和文件夹");
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                Path path = fileStatus.getPath();

                System.out.print(fileStatus.isDirectory() ? "d " : "- ");
                System.out.println(path);
            }
            System.out.println();

            /*
             * 过滤掉指定的文件
             */
            System.out.println("过滤掉指定的文件");
            FileStatus[] fileStatusFilters = fileSystem.listStatus(new Path("/"), path -> !path.getName().startsWith("."));
            for (FileStatus fileStatus : fileStatusFilters) {
                Path path = fileStatus.getPath();
                System.out.println(path);
            }
        }

    }

}
