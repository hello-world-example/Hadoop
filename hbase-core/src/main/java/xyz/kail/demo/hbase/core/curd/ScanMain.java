package xyz.kail.demo.hbase.core.curd;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import xyz.kail.demo.hbase.core.util.HBaseUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 * count "rel_boss_operate_log_v1" 12042
 * count "test_boss_operate_log_v1" 1178
 *
 * @author Kail
 */
public class ScanMain {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtils.getConnection();

        Table relBossOperateLogV1 = connection.getTable(TableName.valueOf("test"));

        Scan get = new Scan(Bytes.toBytes("k1"));
        get.setMaxVersions(); // 设置读取所有版本
//        get.setTimeStamp(1520143352466L); // 查询指定时间戳的数据

        ResultScanner scanner = relBossOperateLogV1.getScanner(get);

        Iterator<Result> iterator = scanner.iterator();
        for (;iterator.hasNext();){
            Result result = iterator.next();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                HBaseUtils.printCell(cell);
            }
        }

        HBaseUtils.close(connection);
    }

}
