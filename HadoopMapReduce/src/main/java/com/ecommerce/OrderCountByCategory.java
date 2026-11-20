package com.ecommerce;

import java.io.IOException;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Assignment operation 1: order count by product category.
 *
 * ORDER DEFINITION (established in Section 3.F of the preprocessing notebook):
 * one row-level Order_ID represents one transaction/order line. The ID prefix
 * is a customer order-group (1:1 with Customer_ID) and must NOT be counted as
 * an order. The prepared input has one unique Order_ID per row, validated in
 * the notebook, so each valid parsed row contributes exactly one order.
 */
public class OrderCountByCategory {

    public static class CategoryMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text categoryKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(EcommerceSchema.Counters.RECORDS_READ).increment(1);

            CSVRecord record = EcommerceSchema.parseLine(value.toString());
            if (record == null) {
                context.getCounter(EcommerceSchema.Counters.RECORDS_INVALID).increment(1);
                return;
            }

            String category = EcommerceSchema.requiredText(
                    record, EcommerceSchema.IDX_PRODUCT_CATEGORY);
            String orderId = EcommerceSchema.requiredText(
                    record, EcommerceSchema.IDX_ORDER_ID);

            if (category == null || orderId == null) {
                context.getCounter(EcommerceSchema.Counters.RECORDS_INVALID).increment(1);
                return;
            }

            categoryKey.set(category);
            context.write(categoryKey, ONE);
            context.getCounter(EcommerceSchema.Counters.RECORDS_PROCESSED).increment(1);
        }
    }

    public static class CategoryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            result.set(total);
            context.write(key, result);   // output: category <tab> order_count
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: OrderCountByCategory <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Order count by product category");
        job.setJarByClass(OrderCountByCategory.class);
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
