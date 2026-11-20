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
 * Assignment operation 3: order count by payment method.
 *
 * Uses the same order definition as OrderCountByCategory: the row-level
 * Order_ID is one transaction; the Order_ID prefix is a customer order-group
 * and is never counted as an order.
 */
public class OrderCountByPayment {

    public static class PaymentMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text paymentKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(EcommerceSchema.Counters.RECORDS_READ).increment(1);

            CSVRecord record = EcommerceSchema.parseLine(value.toString());
            if (record == null) {
                context.getCounter(EcommerceSchema.Counters.RECORDS_INVALID).increment(1);
                return;
            }

            String payment = EcommerceSchema.requiredText(
                    record, EcommerceSchema.IDX_PAYMENT_METHOD);
            String orderId = EcommerceSchema.requiredText(
                    record, EcommerceSchema.IDX_ORDER_ID);

            if (payment == null || orderId == null) {
                context.getCounter(EcommerceSchema.Counters.RECORDS_INVALID).increment(1);
                return;
            }

            paymentKey.set(payment);
            context.write(paymentKey, ONE);
            context.getCounter(EcommerceSchema.Counters.RECORDS_PROCESSED).increment(1);
        }
    }

    public static class PaymentReducer
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
            context.write(key, result);   // output: payment_method <tab> order_count
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: OrderCountByPayment <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Order count by payment method");
        job.setJarByClass(OrderCountByPayment.class);
        job.setMapperClass(PaymentMapper.class);
        job.setReducerClass(PaymentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
