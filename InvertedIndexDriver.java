package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexDriver {

    public static void main(String[] args) throws Exception {

        // Ensure input and output paths are provided
        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index - Age, Sex, Cholesterol");

        job.setJarByClass(InvertedIndexDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MapperInvertedIndex.class);
        job.setReducerClass(ReducerInvertedIndex.class);

        // Set output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
