
package com.xjm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFMR {

    private static int taskNum = 3;
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf); 
            job.setJobName("TFIDF-job");
            job.setJarByClass(TFIDFMR.class);
            job.setMapperClass(TFIDFMR.TFIDFMapper.class);    
            job.setPartitionerClass(TFIDFMR.TFIDFPartitioner.class);  
            job.setNumReduceTasks(taskNum);
            job.setReducerClass(TFIDFMR.TFIDFReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //
            String inputPath1 = args[0] + "TF";
            String inputPath2 = args[0] + "IDF";
            String outputPath = args[1] + "TFIDF";
            Path fileOutPath = new Path(outputPath);
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(fileOutPath)) {
                fileSystem.delete(fileOutPath, true);
            }  
            FileInputFormat.addInputPaths(job, String.join(",", inputPath1, inputPath2));
            FileOutputFormat.setOutputPath(job, fileOutPath);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String str1 = tokenizer.nextToken();
            String str2 = tokenizer.nextToken();
            if (str1.indexOf(":") != -1){
                String str3 = str1.split(":")[0];
                String str4 = str1.split(":")[1];
                context.write(new Text(str3), new Text(String.join("#", str4, str2)));
            }
            else
                context.write(new Text(str1), new Text(str2));
        }
    }
    
    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text workname = new Text();
        private Text wordval = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            double idf = 1.0;
            ArrayList <String> list = new ArrayList<String> ();
            for (Text val : values) {
                String str = val.toString();
                if (str.indexOf("#") != -1)
                    list.add(str);
                else
                    idf = Double.parseDouble(str);
            }
            for(String str:list){
                workname.set(str.split("#")[0]);
                int tf = Integer.parseInt(str.split("#")[1]);
                String tfidf = String.valueOf(idf*tf);
                wordval.set(String.join(" ",key.toString(),tfidf));
                context.write(workname, wordval);
            }           
        }
    }

    public static class TFIDFPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String word = key.toString();
            numPartitions = taskNum;
            return Math.abs((word.hashCode() * 127) % numPartitions);
        }
    }
}
