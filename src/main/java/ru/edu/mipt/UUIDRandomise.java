package ru.edu.mipt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static java.lang.Math.random;
import static org.apache.commons.lang.StringUtils.join;

public class UUIDRandomise {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UUIDRandomise.class);
        job.setMapperClass(UUIDMapper.class);
        job.setCombinerClass(UUIDCombiner.class);
        job.setReducerClass(UUIDReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UUIDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String str : value.toString().split("\\s")) {
                int random = (int) (random() * Integer.MAX_VALUE);
                context.write(new IntWritable(random), new Text(str));
            }
        }
    }

    public static class UUIDCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

        private static final IntWritable KEY = new IntWritable(0);

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(KEY, value);
        }

    }

    public static class UUIDReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        private static final int MIN = 1;
        private static final int MAX = 5;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();

            Iterator<Text> iterator = values.iterator();

            for (int i = 0; i < new Random().nextInt(MAX - MIN) + MIN; i++)
                list.add(iterator.next().toString());

            context.write(new Text(join(list, ",")), NullWritable.get());
        }
    }
}