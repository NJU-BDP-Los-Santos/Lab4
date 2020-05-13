package sample;



import basic.IntPair;
import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class Main // 主函数（类）
{
    // 需要自定义类型进行表示
    int a = 0;
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 3) {
                System.err.println("Please Use the command: <input path> <output path> <sample path>");
                System.exit(2);
            }

            Job job = new Job(conf, "Second-Sort-Sample");
            job.setJarByClass(Main.class);
            job.setMapperClass(ReaderMapper.class);
            job.setReducerClass(JoinReducer.class);
//            job.setCombinerClass(CombinerSameWordDoc.class);
            job.setPartitionerClass(TotalOrderPartitioner.class);
            job.setNumReduceTasks(2);
            job.setGroupingComparatorClass(XGroup.class);
            job.setOutputKeyClass(IntPairS.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(PairInput.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
            InputSampler.RandomSampler<IntPairS, NullWritable> sampler = new InputSampler.RandomSampler<IntPairS, NullWritable>(0.1, 100, 2);
            InputSampler.writePartitionFile(job, sampler);
//            InputFormat<IntPairS,NullWritable> format = new PairInput();
//            Object[] keys = sampler.getSample(format, job);
//            for (Object key : keys)
//            {
//                IntPairS x = (IntPairS)key;
//                x.print();
//            }
//            Path catchPath =new Path("/cache");
//            URI partitionUri= new URI(catchPath.toString()+ "#myCache");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class ReaderMapper extends Mapper<IntPairS, NullWritable, IntPairS, NullWritable>
            // 用于读取文件中的信息，并且组织成为自定义的类型
    {
        @Override
        protected void map(IntPairS key, NullWritable value, Context context) throws IOException, InterruptedException
        {
            context.write(key, value);
        }
    }

    public static class PidPartitioner extends HashPartitioner<IntPairS, NullWritable>
    {
        @Override
        public int getPartition(IntPairS key, NullWritable value,
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
            super(IntPairS.class, true);
        }
        public int compare(WritableComparable left, WritableComparable right)
        {
            IntPairS l = (IntPairS) left;
            IntPairS r = (IntPairS) right;
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

    public static class JoinReducer extends Reducer<IntPairS, NullWritable, Text, NullWritable>
    {
        @Override
        protected void reduce(IntPairS key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            // 首先找到product对应的那个key
            for (NullWritable value: values)
            {
                context.write(key.toText(), value);
            }
        }
    }
}
