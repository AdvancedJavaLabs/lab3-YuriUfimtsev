package com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortedDataToFileReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    private boolean isHeaderWritten;

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (!isHeaderWritten) {
            String header = String.format("%-20s %-15s %-10s", "Category", "Revenue", "Quantity");
            context.write(null, new Text(header));
            isHeaderWritten = true;
        }

        double revenue = key.get();

        for (Text value : values) {
            String[] parts = value.toString().split("-");
            String formattedResult = String.format("%-20s %-15.2f %-10s", parts[0], revenue, parts[1]);
            context.write(null, new Text(formattedResult));
        }
    }
}
