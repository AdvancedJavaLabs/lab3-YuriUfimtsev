package com.yu_ufimtsev.itmo.sales_data_analysis.aggregate_stage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CategoryStatsReducer extends Reducer<Text,Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalRevenue = 0;
        int totalQuantity = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(";");
            totalRevenue +=  Double.parseDouble(parts[0]);
            totalQuantity += Integer.parseInt(parts[1]);
        }

        context.write(key, new Text(String.format("%.2f\t%d", totalRevenue, totalQuantity)));
    }
}
