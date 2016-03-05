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
        job.setNumReduceTasks(8);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UUIDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(new Random().nextInt()), value);
        }
    }

    public static class UUIDCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int basket = new Random().nextInt(context.getNumReduceTasks()) + 1;
            for (Text value : values)
                context.write(new IntWritable(basket), value);
        }

    }

    public static class UUIDReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        private static final int MIN = 1;
        private static final int MAX = 5;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                List<String> list = new ArrayList<>();
                int random = new Random().nextInt(MAX - MIN) + MIN;
                int i = 0;
                while (i < random && iterator.hasNext()) {
                    list.add(iterator.next().toString());
                    i++;
                }
                context.write(new Text(join(list, ",")), NullWritable.get());
            }
        }
    }
}