package cn.itcast.hdfs.demo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsOperateStudy {

    @Test
    public void getHdfsFile()
    {
        System.out.println("hello world");
       /* URL.setURLStreamHandlerFactory();*/
        //第一步：注册hdfs 的url，让java代码能够识别hdfs的url形式
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        InputStream inputStream = null;
        FileOutputStream outputStream =null;
        //定义文件访问的url地址
        String url = "hdfs://node01:8020/test/input/install.log";
        //打开文件输入流
        try {
            inputStream = new URL(url).openStream();
            outputStream = new FileOutputStream(new File("c:\\hello.txt"));
            IOUtils.copy(inputStream, outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }


    @Test
    public void getFileSystem() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }
}
