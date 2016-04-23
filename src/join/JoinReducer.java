package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuyong on 16-4-22.
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    public static final String LEFT_FILENAME = "student";
    public static final String RIGHT_FILENAME = "class";
    public static final String LEFT_FILENAME_FLAG = "l";
    public static final String RIGHT_FILENAME_FLAG = "r";
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        List<String> studentClassNames = new ArrayList<>();
        String studentName = "";
        while (iterator.hasNext()){
            String[] info = iterator.next().toString().split(" ");
            if(info[1].equals(LEFT_FILENAME_FLAG)){
                studentName = info[0];
            }else if(info[1].equals(RIGHT_FILENAME_FLAG)){
                studentClassNames.add(info[0]);
            }
        }
        for(int i = 0; i< studentClassNames.size(); i++){
            context.write(new Text(studentName), new Text(studentClassNames.get(i)));
        }

    }

}
