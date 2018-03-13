package xyz.kail.demo.hbase.core.curd;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import xyz.kail.demo.hbase.core.util.HBaseUtils;

/**
 * count "rel_boss_operate_log_v1" 12042
 * count "test_boss_operate_log_v1" 1178
 *
 * @author kaibin.yang@ttpai.cn
 */
public class GetMain {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtils.getConnection();

        Table relBossOperateLogV1 = connection.getTable(TableName.valueOf("test"));

        Get get = new Get(Bytes.toBytes("r1"));
        get.setMaxVersions(); // 设置读取所有版本
        get.addColumn(Bytes.toBytes("e"), Bytes.toBytes("c1"));
        get.setTimeStamp(12L); // 查询指定时间戳的数据

        Result result = relBossOperateLogV1.get(get);

        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            HBaseUtils.printCell(cell);
        }

        HBaseUtils.close(connection);
    }

}
