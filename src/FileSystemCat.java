import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by liuyong on 16-4-21.
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9000/input/test";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out,4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
