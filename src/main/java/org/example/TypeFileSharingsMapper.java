package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TypeFileSharingsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tk = new StringTokenizer(value.toString());
        String token = tk.nextToken();

        if(token.contains("Type")) {
            palavra.set(tk.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", ""));
            context.write(palavra, numeroUm);
        }
    }
}
