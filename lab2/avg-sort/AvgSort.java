package avgsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class AvgSort {
    // Inverted Index:
    public static class IIMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String temp = new String();
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                temp = itr.nextToken();
                Text word = new Text();
                word.set(temp + "#" + fileName);
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class IICombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IIPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String term = new String();
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class IIReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            word1.set("[" + key.toString().split("#")[0] + "]");
            temp = key.toString().split("#")[1];
            for (IntWritable val:values) {
                sum += val.get();
            }
            word2.set(" " + temp + ":" + sum + ";");
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
                StringBuilder out = new StringBuilder();
                long count_word = 0;
                long count_file = 0;
                for (String p: postingList) {
                    count_word += Long.parseLong(p.substring(p.indexOf(":") + 1, p.indexOf(";")));
                    count_file += 1;
                }
                Double average_occurrence = (double)count_word / count_file;
                out.append(String.valueOf(average_occurrence));
                if (count_word > 0)
                    context.write(CurrentItem, new Text(out.toString()));
                postingList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long count_word = 0;
            long count_file = 0;
            for (String p: postingList) {
                count_word += Long.parseLong(p.substring(p.indexOf(":") + 1, p.indexOf(";")));
                count_file += 1;
            }
            Double average_occurrence = (double)count_word / count_file;
            out.append(String.valueOf(average_occurrence));
            if (count_word > 0)
                context.write(CurrentItem, new Text(out.toString()));
        }
    }

    // AvgSort:
    public static class AvgSortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String term = line.split("\t")[0];
            String AvgOccurStr = line.split("\t")[1];
            Double AvgOccur = Double.parseDouble(AvgOccurStr);
            context.write(new DoubleWritable(AvgOccur), new Text(term));
        }
    }

    // main:
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Invalid Path!");
            System.exit(2);
        }

        // job1:
        Job job1 = Job.getInstance(conf, "CalculateAvgOccur");
        // set class:
        job1.setJarByClass(AvgSort.class);
        job1.setMapperClass(IIMapper.class);
        job1.setCombinerClass(IICombiner.class);
        job1.setPartitionerClass(IIPartitioner.class);
        job1.setReducerClass(IIReducer.class);
        // map:
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        // output:
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        // path:
        FileInputFormat.addInputPath(job1, new Path(args[0])); // dataset
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); // temp dir
        // completion:
        job1.waitForCompletion(true);

        // job2:
        Job job2 = Job.getInstance(conf, "AvgOccurSort");
        // set class:
        job2.setJarByClass(AvgSort.class);
        job2.setMapperClass(AvgSortMapper.class);
        job2.setPartitionerClass(TotalOrderPartitioner.class);
        // map:
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // output:
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Text.class);
        // path:
        FileInputFormat.addInputPath(job2, new Path(args[1])); // temp dir
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // avgSort output
        // completion:
        job2.waitForCompletion(true);
    }
}
