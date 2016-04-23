import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by liuyong on 16-4-21.
 */
public class MyCat {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream in = null;
        try {
            in = new URL("hdfs://master:9000/input/test").openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
