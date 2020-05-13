package sample;

//import com.google.common.io.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.mockito.internal.matchers.Null;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class PairRecordReader extends RecordReader<IntPairS, NullWritable>
{
    private FileSplit fs ;
    private IntPairS key;
    private NullWritable value;
    private LineReader reader;

    private String fileName;

    public PairRecordReader()
    {

    }

    //初始化方法
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs = (FileSplit) split;
        fileName = fs.getPath().getName();
        Path path = fs.getPath();
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem system = path.getFileSystem(conf);
        FSDataInputStream in = system.open(path);
        reader = new LineReader(in);
    }


    //知识点1:这个方法会被调用多次   这个方法的返回值如果是true就会被调用一次
    // 知识点2:每当nextKeyValue被调用一次 ，getCurrentKey，getCurrentValue也会被跟着调用一次
    //知识点3:getCurrentKey,getCurrentValue给Map传key,value
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {

        Text tmp = new Text();
        int length = reader.readLine(tmp);
        if(length == 0)
        {
            return false;
        }
        else
        {
            StringTokenizer tokens = new StringTokenizer(tmp.toString());
//            System.out.println(tmp.toString());
            int[] data = new int[2];

            int i = 0;
            while(tokens.hasMoreTokens())
            {
                data[i] = Integer.parseInt(tokens.nextToken());
                ++i;
            }
            key = new IntPairS(data[0], data[1]);
            value = NullWritable.get();
            return true;
        }
    }

    @Override
    public IntPairS getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if(reader!=null){
            reader.close();
        }
    }
}
