package com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingDoubleComparator extends WritableComparator {

    public DescendingDoubleComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -((DoubleWritable) a).compareTo((DoubleWritable) b);
    }
}
