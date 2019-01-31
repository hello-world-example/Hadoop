package xyz.kail.demo.hadoop.hdfs.curd;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import xyz.kail.demo.hadoop.hdfs.HdfsTool;

import java.io.IOException;
import java.util.stream.Stream;

public class MkdirsMain {

    private static final String HOME_PATH = "/user/kail";

    public static void main(String[] args) throws IOException {
        try (FileSystem fileSystem = HdfsTool.getFileSystem()) {

            Path testPath = new Path(HOME_PATH, "test");

            /*
             * 创建文件夹
             */
            boolean success = fileSystem.mkdirs(new Path(HOME_PATH, "test"));
            System.out.println(testPath + " 创建 " + (success ? "成功" : "失败"));

            /*
             * 文件使用情况
             */
            FsStatus fsStatus = fileSystem.getStatus(testPath);
            System.out.println("Used：" + fsStatus.getUsed());
            System.out.println("Remaining：" + fsStatus.getRemaining());
            System.out.println("Capacity：" + fsStatus.getCapacity());

            /*
             * 打印文件权限
             */
            FileStatus[] fileStatuses = fileSystem.listStatus(testPath.getParent(), path -> path.getName().equals(testPath.getName()));
            Stream.of(fileStatuses).forEach(fileStatus -> {
                System.out.println(fileStatus.getPermission());

            });

            /*
             * 设置权限
             */
            FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fileSystem.setPermission(testPath, fsPermission);

            /*
             * 设置权限 2
             */
            fsPermission = new FsPermission((short) 755);
            fileSystem.setPermission(testPath, fsPermission);


        }
    }


}
