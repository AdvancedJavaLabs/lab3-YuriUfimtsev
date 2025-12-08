package com.yu_ufimtsev.itmo.sales_data_analysis.aggregate_stage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RevenueCalculationMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("transaction_id")) {
            return;
        }

        String[] parts = line.split(",");
        if (parts.length != 5) {
            return;
        }

        String categoryName = parts[2];

        double price = Double.parseDouble(parts[3]);
        int quantity = Integer.parseInt(parts[4]);
        double revenue = price * quantity;

        context.write(
                new Text(categoryName),
                new Text(String.format("%s;%s", revenue, quantity))
        );
    }
}
