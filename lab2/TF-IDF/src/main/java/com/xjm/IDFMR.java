package com.xjm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDFMR {

    private static int taskNum = 3;

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJobName("IDF-job");
            job.setJarByClass(IDFMR.class);
            job.setMapperClass(IDFMR.IDFMapper.class);
            job.setCombinerClass(IDFMR.IDFCombiner.class);
            job.setPartitionerClass(IDFMR.IDFPartitioner.class);
            job.setNumReduceTasks(taskNum);
            job.setReducerClass(IDFMR.IDFReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            Path inputPath = new Path(args[0]);
            // 计算文档总数并加入设置
            job.setProfileParams(String.valueOf(getFilesNum(conf, inputPath)));
            String outputPath = args[1] + "IDF";
            Path fileOutPath = new Path(outputPath);
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(fileOutPath)) {
                fileSystem.delete(fileOutPath, true);
            }
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, fileOutPath);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int getFilesNum(Configuration conf, Path inputPath) throws FileNotFoundException, IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus status[] = fs.listStatus(inputPath);
        return status.length;
    }

    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
        private final Text one = new Text("1");
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IDFCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text one = new Text("1");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            context.write(key, one);
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            int fileCount = 0;
            for (Text value : values) {
                fileCount += Integer.parseInt(value.toString());
            }
            int totalFiles = Integer.parseInt(context.getProfileParams());
            double idf = Math.log10(1.0 * totalFiles / (fileCount + 1));
            context.write(key, new Text(String.valueOf(idf)));
        }
    }

    public static class IDFPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String word = key.toString();
            numPartitions = taskNum;
            return Math.abs((word.hashCode() * 127) % numPartitions);
        }
    }

}
