package com.ecommerce;

import java.io.IOException;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Assignment operation 2: total (net) sales by product category.
 *
 * The original implementation used Unit_Price * Quantity, i.e. GROSS sales,
 * which ignored discounts. This version uses Total_Amount (net sales) and
 * independently verifies Total_Amount = Gross_Amount - Discount_Amount.
 * Monetary values are summed as integer cents to avoid floating-point drift.
 */
public class SalesByCategory {

    public static class SalesMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private final Text categoryKey = new Text();
        private final LongWritable salesCents = new LongWritable();

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
            Long unitPriceCents = EcommerceSchema.parseNonNegativeMoneyToCents(
                    record, EcommerceSchema.IDX_UNIT_PRICE);
            Long quantity = EcommerceSchema.parseNonNegativeInteger(
                    record, EcommerceSchema.IDX_QUANTITY);
            Long discountCents = EcommerceSchema.parseNonNegativeMoneyToCents(
                    record, EcommerceSchema.IDX_DISCOUNT_AMOUNT);
            Long netCents = EcommerceSchema.parseNonNegativeMoneyToCents(
                    record, EcommerceSchema.IDX_TOTAL_AMOUNT);

            if (category == null) {
                context.getCounter(EcommerceSchema.Counters.RECORDS_INVALID).increment(1);
                return;
            }
            if (unitPriceCents == null || quantity == null
                    || discountCents == null || netCents == null) {
                context.getCounter(EcommerceSchema.Counters.INVALID_NUMERIC).increment(1);
                return;
            }

            // Financial validation rule from the preprocessing investigation.
            long expectedNetCents = unitPriceCents * quantity - discountCents;
            if (expectedNetCents != netCents) {
                context.getCounter(EcommerceSchema.Counters.FINANCIAL_MISMATCH).increment(1);
                return;
            }

            categoryKey.set(category);
            salesCents.set(netCents);
            context.write(categoryKey, salesCents);
            context.getCounter(EcommerceSchema.Counters.RECORDS_PROCESSED).increment(1);
        }
    }

    public static class SalesReducer
            extends Reducer<Text, LongWritable, Text, Text> {

        private final Text formattedTotal = new Text();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long totalCents = 0L;
            for (LongWritable value : values) {
                totalCents += value.get();
            }
            formattedTotal.set(EcommerceSchema.formatCents(totalCents));
            context.write(key, formattedTotal);   // output: category <tab> net_sales
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SalesByCategory <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Net sales by product category");
        job.setJarByClass(SalesByCategory.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
