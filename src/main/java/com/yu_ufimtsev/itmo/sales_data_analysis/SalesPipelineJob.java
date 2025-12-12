package com.yu_ufimtsev.itmo.sales_data_analysis;

import com.yu_ufimtsev.itmo.sales_data_analysis.aggregate_stage.CategoryStatsReducer;
import com.yu_ufimtsev.itmo.sales_data_analysis.aggregate_stage.RevenueCalculationMapper;
import com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage.DescendingDoubleComparator;
import com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage.SortByRevenueMapper;
import com.yu_ufimtsev.itmo.sales_data_analysis.sort_stage.SortedDataToFileReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SalesPipelineJob {

    private static final String REPORT_CSV_PATH = "/opt/hadoop/reports/results.csv";

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Please, use format:" +
                    " SalesPipelineJob <inputDataDir> <intermediateDir> <outputDir> <splitMB> <reducersCount>");
            System.exit(1);
        }

        long splitMb = Long.parseLong(args[3]);
        long splitSizeInBytes = splitMb * 1024 * 1024;
        int reducersCount = Integer.parseInt(args[4]);

        String inputPath = args[0];
        String intermediatePath = args[1];
        String outputPath = args[2];

        Configuration configuration = new Configuration();

        long aggregationJobDuration = prepareAndExecuteAggregationSalesJob(
                configuration, splitSizeInBytes, reducersCount, inputPath, intermediatePath
        );
        System.out.println("--------------------------------------------------");
        System.out.printf("Sales aggregation job time: %d ms\n", aggregationJobDuration);
        System.out.println("--------------------------------------------------");

        long sortingJobDuration = prepareAndExecuteSortingCategoriesJob(
                configuration, intermediatePath, outputPath
        );
        System.out.println("--------------------------------------------------");
        System.out.printf("Categories sorting job time: %d ms\n", sortingJobDuration);
        System.out.println("--------------------------------------------------");

        long totalTime = aggregationJobDuration + sortingJobDuration;
        System.out.println("--------------------------------------------------");
        System.out.printf("Total jobs time: %d ms\n", totalTime);
        System.out.println("--------------------------------------------------");

        appendResultsToCsv(splitMb, reducersCount, aggregationJobDuration, sortingJobDuration, totalTime);
    }

    private static long prepareAndExecuteAggregationSalesJob(
            Configuration configuration,
            long splitSize, int reducersCount, String inputPath, String intermediatePath) throws Exception {

        Job aggregationJob = Job.getInstance(configuration);
        aggregationJob.setJobName("sales-aggregation");

        aggregationJob.setJarByClass(SalesPipelineJob.class);
        aggregationJob.setMapperClass(RevenueCalculationMapper.class);
        aggregationJob.setReducerClass(CategoryStatsReducer.class);

        aggregationJob.setMapOutputKeyClass(Text.class);
        aggregationJob.setMapOutputValueClass(Text.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(Text.class);

        FileInputFormat.setMaxInputSplitSize(aggregationJob, splitSize);
        aggregationJob.setNumReduceTasks(reducersCount);

        FileInputFormat.addInputPath(aggregationJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(aggregationJob, new Path(intermediatePath));

        long startAggregationJob = System.currentTimeMillis();
        boolean isAggregationJobSuccessful = aggregationJob.waitForCompletion(true);
        long endAggregationJob = System.currentTimeMillis();
        long aggregationJobDuration = endAggregationJob - startAggregationJob;

        if (!isAggregationJobSuccessful) {
            System.err.println("Aggregation job failed");
            System.exit(1);
        }

        return aggregationJobDuration;
    }

    private static long prepareAndExecuteSortingCategoriesJob(
            Configuration configuration, String intermediatePath, String outputPath) throws Exception {

        Job sortingCategoriesJob = Job.getInstance(configuration, "sorting-categories");
        sortingCategoriesJob.setJobName("sorting-categories");

        sortingCategoriesJob.setJarByClass(SalesPipelineJob.class);
        sortingCategoriesJob.setMapperClass(SortByRevenueMapper.class);
        sortingCategoriesJob.setReducerClass(SortedDataToFileReducer.class);

        sortingCategoriesJob.setMapOutputKeyClass(DoubleWritable.class);
        sortingCategoriesJob.setMapOutputValueClass(Text.class);
        sortingCategoriesJob.setOutputKeyClass(Text.class);
        sortingCategoriesJob.setOutputValueClass(Text.class);

        sortingCategoriesJob.setSortComparatorClass(DescendingDoubleComparator.class);
        sortingCategoriesJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(sortingCategoriesJob, new Path(intermediatePath));
        FileOutputFormat.setOutputPath(sortingCategoriesJob, new Path(outputPath));

        long startSortingJob = System.currentTimeMillis();
        boolean isSortingJobSuccessful = sortingCategoriesJob.waitForCompletion(true);
        long endSortingJob = System.currentTimeMillis();
        long sortingJobDuration = endSortingJob - startSortingJob;

        if (!isSortingJobSuccessful) {
            System.err.println("Sorting job failed");
            System.exit(1);
        }

        return sortingJobDuration;
    }

    private static void appendResultsToCsv(long splitMB, long reducersCount, long aggregationMs,
                                           long sortingMs, long totalPipelineMs) {

        try {
            File reportFile = new File(REPORT_CSV_PATH);
            boolean isReportFileNew = !reportFile.exists();

            try (FileWriter fileWriter = new FileWriter(reportFile, true)) {
                if (isReportFileNew) {
                    fileWriter.write("splitMB,reducersCount,aggregationMs,sortingMs,totalMs,timestamp\n");
                }

                fileWriter.write(String.format(
                        "%d,%d,%d,%d,%d,%s\n",
                        splitMB, reducersCount,
                        aggregationMs, sortingMs, totalPipelineMs,
                        java.time.LocalDateTime.now()
                ));
            }
            System.out.println("CSV report successfully updated");

        } catch (IOException e) {
            System.err.println("Failed writing to CSV report: " + e.getMessage());
        }
    }
}
