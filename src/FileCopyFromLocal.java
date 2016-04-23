import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by liuyong on 16-4-21.
 */
public class FileCopyFromLocal {
    public static void main(String[] args) throws IOException {
        String source = "/home/liuyong/abc";
        String dest = "hdfs://master:9000/input/abc";
        InputStream in = new BufferedInputStream(new FileInputStream(source));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dest), conf);
        OutputStream out = fs.create(new Path(dest));
        IOUtils.copyBytes(in, out, 4096, true);

    }
}
