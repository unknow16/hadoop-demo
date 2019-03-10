package com.fuyi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

public class HdfsClient {

    /**
     * 测试创建目录
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://120.78.193.62:9000"), configuration, "fuyi");

        // 2 创建目录
        fs.mkdirs(new Path("/aaa/bbb/ccc"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 测试文件上传: 报错原因：aliyun上nameNode返回了dataNode的内网ip
     *
     * 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
     */
    @Test
    public void testCopyFromLocalFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        System.setProperty("HADOOP_USER_NAME", "fuyi");
        fs.copyFromLocalFile(new Path("d:/dubbo.xsd"), new Path("/"));
        fs.close();
        System.out.println("over!");
    }
}
