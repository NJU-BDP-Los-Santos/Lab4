package basic;



import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Main // 主函数（类）
{
    // 需要自定义类型进行表示

    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Please Use the command: <input path> <output path>");
                System.exit(2);
            }

            Job job = new Job(conf, "Second-Sort-Basic");
            job.setJarByClass(Main.class);
            job.setMapperClass(ReaderMapper.class);
            job.setReducerClass(JoinReducer.class);
//            job.setCombinerClass(CombinerSameWordDoc.class);
            job.setPartitionerClass(PidPartitioner.class);
            job.setNumReduceTasks(2);
            job.setGroupingComparatorClass(XGroup.class);
            job.setOutputKeyClass(IntPair.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class ReaderMapper extends Mapper<LongWritable, Text, IntPair, NullWritable>
            // 用于读取文件中的信息，并且组织成为自定义的类型
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer tokens = new StringTokenizer(value.toString());
            System.out.println(value.toString());
            int[] data = new int[2];

            int i = 0;
            while(tokens.hasMoreTokens())
            {
                data[i] = Integer.parseInt(tokens.nextToken());
                ++i;
            }

            // 这里对i的情况进行分类讨论
            IntPair pair = new IntPair(data[0], data[1]);

            context.write(pair, NullWritable.get());
        }
    }

    public static class PidPartitioner extends HashPartitioner<IntPair, NullWritable>
    {
        @Override
        public int getPartition(IntPair key, NullWritable value,
                                int numPartitions)
        {
            int len_of_part = 10 / numPartitions;
            if (len_of_part * numPartitions < 10)
            {
                len_of_part += 1;
            }
            int part = key.getX() / len_of_part;
            if (part >= numPartitions)
                part = numPartitions - 1;
            return part;
        }
    }

    public static class XGroup extends WritableComparator
    {
        public XGroup()
        {
            super(IntPair.class, true);
        }
        public int compare(WritableComparable left, WritableComparable right)
        {
            IntPair l = (IntPair) left;
            IntPair r = (IntPair) right;
            int l_x = l.getX();
            int r_x = r.getX();
            if (l_x == r_x)
                return 0;
            else if (l_x > r_x)
                return 1;
            else
                return -1;
        }
    }

    public static class JoinReducer extends Reducer<IntPair, NullWritable, Text, NullWritable>
    {
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            // 首先找到product对应的那个key
            for (NullWritable value: values)
            {
                context.write(key.toText(), value);
            }
        }
    }
}
