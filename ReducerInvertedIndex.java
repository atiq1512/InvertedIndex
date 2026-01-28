package com.example.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerInvertedIndex extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<Integer> lines = new ArrayList<>();

        // Collect all line numbers
        for (IntWritable val : values) {
            lines.add(val.get());
        }

        // Sort line numbers
        Collections.sort(lines);

        // Convert list of integers to a comma-separated string
        StringBuilder lineList = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            lineList.append(lines.get(i));
            if (i != lines.size() - 1) {
                lineList.append(",");
            }
        }

        // Output format: key → line numbers
        context.write(key, new Text(lineList.toString()));
    }
}
