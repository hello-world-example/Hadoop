# 端口变化

> http://localhost:9870/conf 页面查看对应的端口配置

| 应用                  | 配置                                 | Haddop 2.x port | Haddop 3 port |
| --------------------- | :----------------------------------- | --------------- | ------------- |
| **NameNode**          | `fs.defaultFS`                       | 8020            | 9820          |
| **NameNode HTTP UI**  | `dfs.namenode.http-address`          | 50070           | 9870          |
| NameNode HTTPs UI     | dfs.namenode.https-address           | 50470           | 9871          |
|                       |                                      |                 |               |
| Secondary NN HTTP UI  | dfs.namenode.secondary.https-address | 50091           | 9869          |
| Secondary NN HTTPs UI | dfs.namenode.secondary.http-address  | 50090           | 9868          |
|                       |                                      |                 |               |
| DataNode              | dfs.datanode.address                 | 50010           | 9866          |
| DataNode IPC          | dfs.datanode.ipc.address             | 50020           | 9867          |
| DataNode HTTP UI      | dfs.datanode.http.address            | 50075           | 9864          |
| DataNode HTTPs UI     | dfs.datanode.https.address           | 50475           | 9865          |



## Read More

- [Hadoop2.x与Hadoop3.x的默认端口变化](https://blog.csdn.net/qq_31454379/article/details/105439752)