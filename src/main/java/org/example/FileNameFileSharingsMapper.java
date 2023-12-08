package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FileNameFileSharingsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tk = new StringTokenizer(value.toString());
        String token = tk.nextToken();

        if(token.contains("FileName")) {
            String[] partes = tk.nextToken().split("\\.");

            if(partes.length > 1) {
                String ultimoItem = partes[partes.length - 1];
                palavra.set(ultimoItem.toLowerCase().replaceAll("[^a-zA-Z]", ""));

                context.write(palavra, numeroUm);
            }
        }
    }
}
