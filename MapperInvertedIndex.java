package com.example.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperInvertedIndex extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable lineNumber = new IntWritable();
    private int currentLine = 0; // track line numbers

    // Only the columns present in your dataset
    private String[] columns = {"age", "sex", "chol"};

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip empty lines and header
        if (line.isEmpty() || line.toLowerCase().startsWith("age")) {
            return;
        }

        currentLine++; // increment line number
        String[] fields = line.split("\\s+"); // split by spaces or tabs

        // Map each field with column name
        for (int i = 0; i < fields.length && i < columns.length; i++) {
            String k = columns[i] + ":" + fields[i]; // e.g., "age:52"
            word.set(k);
            lineNumber.set(currentLine);
            context.write(word, lineNumber);
        }
    }
}
