import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.*;



/**
 * Created by liuyong on 16-4-21.
 */
public class WriterIO {
    public static void main(String[] args) throws IOException {
        IntWritable writable = new IntWritable();
        writable.set(12313);
        byte[] bytes = serialize(writable);
        for(int i = 0; i < bytes.length; i++){
            System.out.println(bytes[i]);
        }
        IntWritable intWritable = new IntWritable();
        deserizlize(intWritable, bytes);
        System.out.println(intWritable.get());
    }
    public static byte[] serialize(Writable writable)throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }
    public static byte[] deserizlize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;

    }
}
