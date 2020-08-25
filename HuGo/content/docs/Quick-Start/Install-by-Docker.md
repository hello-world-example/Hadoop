# Docker 方式安装



## 快速安装

``` bash
$ git clone https://github.com/big-data-europe/docker-hadoop.git
$ cd docker-hadoop
# 选择版本
$ git tag --list
# 切换到制定版本
$ git branch hadoop-3.2.1 2.0.0-hadoop3.2.1-java8
$ git checkout hadoop-3.2.1

# 启动
$ docker-compose up -d

# 退出并删除容器
# $ docker-compose down
```

## 依赖的镜像

> - [Docker 镜像加速](http://kail.xyz/Docker/docs/FAQ/#镜像加速器)

```bash
$ docker images
REPOSITORY                       TAG                       IMAGE ID            CREATED             SIZE
bde2020/hadoop-nodemanager       2.0.0-hadoop3.2.1-java8   4e47dabd148f        6 months ago        1.37GB
bde2020/hadoop-resourcemanager   2.0.0-hadoop3.2.1-java8   3deba4a1885f        6 months ago        1.37GB
bde2020/hadoop-namenode          2.0.0-hadoop3.2.1-java8   839ec11d95f8        6 months ago        1.37GB
bde2020/hadoop-historyserver     2.0.0-hadoop3.2.1-java8   173c52d1f624        6 months ago        1.37GB
bde2020/hadoop-datanode          2.0.0-hadoop3.2.1-java8   df288ee0a7f9        6 months ago        1.37GB
```

## 测试集群

> 访问： http://localhost:9870/explorer.html#/

``` bash
$ docker exec -it namenode bash

$ hadoop fs -ls /
Found 1 items
drwxr-xr-x   - root supergroup          0 2020-08-25 14:46 /rmstate
```



## Read More

- [big-data-europe / docker-hadoop](https://github.com/big-data-europe/docker-hadoop)
- [How to set up a Hadoop cluster in Docker](https://clubhouse.io/developer-how-to/how-to-set-up-a-hadoop-cluster-in-docker/)
- [如何在 Docker 上部署 Hadoop 集群](https://blog.csdn.net/OnedayIlove/article/details/100594618)