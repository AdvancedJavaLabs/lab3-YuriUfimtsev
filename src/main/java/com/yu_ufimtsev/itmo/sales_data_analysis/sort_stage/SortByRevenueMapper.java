package com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByRevenueMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;

        }
        String[] parts = value.toString().split("\\t");
        if (parts.length != 3) {
            return;
        }

        String categoryName = parts[0].trim();

        double revenue = Double.parseDouble(parts[1].trim());
        int quantity = Integer.parseInt(parts[2].trim());

        context.write(
                new DoubleWritable(revenue),
                new Text(String.format("%s-%d", categoryName, quantity))
        );
    }
}
