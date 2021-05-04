
package com.xjm;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFMR{

    private static int taskNum = 3;
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf); 
            job.setJobName("TF-job");
            job.setJarByClass(TFMR.class);
            job.setMapperClass(TFMR.TFMapper.class);    
            job.setCombinerClass(TFMR.TFCombiner.class);
            job.setPartitionerClass(TFMR.TFPartitioner.class);  
            job.setNumReduceTasks(taskNum);
            job.setReducerClass(TFMR.TFReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            String inputPath = args[0];
            String outputPath = args[1];
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TFMapper extends Mapper<Object, Text, Text, Text> {
    
        private final Text one = new Text("1"); 
        private Text word = new Text();
        private String workName = "";
        
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String fileFullName = ((FileSplit)inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            workName = nameSegments[nameSegments.length - 1];
            int idx = workName.indexOf("第");
            if (idx != -1){
                workName = workName.substring(0, idx);
            }
        }
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(String.join(":", tokenizer.nextToken(), workName));
                context.write(word, one);  
            }
        }
        
      
    }
    
    public static class TFCombiner extends Reducer<Text, Text, Text, Text> {      
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            
            if (values == null) {
                return;
            }                 
            int sumCount = 0;
            for (Text val : values) {
                sumCount += Integer.parseInt(val.toString());
            }
            context.write(key, new Text(String.valueOf(sumCount)));
        }
    }
    
    public static class TFReducer extends Reducer<Text, Text, Text, Text> {    
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
            if (values == null) {
                return;
            }   
            int sumCount = 0;
            for (Text val : values) {
                sumCount += Integer.parseInt(val.toString());
            }
            context.write(key, new Text(String.valueOf(sumCount)));
        }
    }
    
    public static class TFPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            numPartitions = taskNum;
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }
    
}
