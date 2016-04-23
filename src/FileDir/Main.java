package FileDir;

import java.util.ArrayList;

/**
 * Created by liuyong on 16-4-21.
 */
public class Main {
    public static void main(String[] args) {
        MakeHDFS makeHDFS = new MakeHDFS();
        //makeHDFS.createDir("hdfs://master:9000/user");
        //makeHDFS.delFile("hdfs://master:9000/input/abc");
        makeHDFS.checkIsExist("hdfs://master:9000/input/abc");
        ArrayList<String> arrayList = makeHDFS.listFIleSystem("hdfs://master:9000/o;utput/");
        for(String file : arrayList){
            System.out.println(file);
        }
    }
}
