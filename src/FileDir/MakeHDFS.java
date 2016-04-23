package FileDir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

/**
 * Created by liuyong on 16-4-21.
 */
public class MakeHDFS {
    //创建目录
    Configuration configuration = new Configuration();
    void createDir(String uri){
        try{
            FileSystem fs = FileSystem.get(URI.create(uri), configuration);
            Path path = new Path(uri);
            fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void delFile(String uri) {
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), configuration);
            Path path = new Path(uri);
            boolean isDelete = fs.delete(path, false);
            System.out.println(isDelete);
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    boolean checkIsExist(String uri){
        try{
            FileSystem fs = FileSystem.get(URI.create(uri), configuration);
            Path path = new Path(uri);
            boolean isExist = fs.exists(path);
            return isExist;
        }catch (IOException e){
            e.printStackTrace();
        }
       return false;
    }
    ArrayList<String> listFIleSystem(String uri){
        ArrayList<String> files = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), configuration);
            Path path = new Path(uri);
            FileStatus fileStatus[] = fs.listStatus(path);
            for(int i = 0;i < fileStatus.length; i++){
                files.add(fileStatus[i].getPath().toString());
            }
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return files;
    }
}
