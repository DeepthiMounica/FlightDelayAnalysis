

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//Avg monthly arrival delay trend.
public class MonthlyDelaysTrend {

    public static class MonthlyDelaysTrendMapper extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, DoubleWritable>{

        @Override
        protected void map(Object key, org.apache.hadoop.io.Text value, Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            if (!value.toString().contains("ARR_DELAY_NEW")) {

                String[] input = value.toString().split(",");

                if(input.length >7){

                    org.apache.hadoop.io.Text month = new org.apache.hadoop.io.Text();
                    try{

                        String str = input[1].trim();

                        String m = "invalid";

                        if(str.equalsIgnoreCase("1")) m = "JAN";
                        else if(str.equalsIgnoreCase("2")) m = "FEB";
                        else if(str.equalsIgnoreCase("3")) m = "MAR";
                        else if(str.equalsIgnoreCase("4")) m = "APR";
                        else if(str.equalsIgnoreCase("5")) m = "MAY";
                        else if(str.equalsIgnoreCase("6")) m = "JUN";
                        else if(str.equalsIgnoreCase("7")) m = "JUL";
                        else if(str.equalsIgnoreCase("8")) m = "AUG";
                        else if(str.equalsIgnoreCase("9")) m = "SEP";
                        else if(str.equalsIgnoreCase("10")) m = "OCT";
                        else if(str.equalsIgnoreCase("11")) m = "NOV";
                        else if(str.equalsIgnoreCase("12")) m = "DEC";

                        month.set(m);

                        double arrDelay = Double.parseDouble(input[28]);

                        if(arrDelay < 60) context.write(month, new DoubleWritable(Double.parseDouble(input[28])));

                    } catch (Exception ex) {
                        //throw ex;
                        //UniqueCarrier.set(input[2]);
                        //context.write(UniqueCarrier, new AverageBean(0, 0));
                    }
                }
            }
        }

    }


    public static class MonthlyDelaysTrendReducer extends Reducer<org.apache.hadoop.io.Text, DoubleWritable, org.apache.hadoop.io.Text, DoubleWritable> {

        @Override
        protected void reduce(org.apache.hadoop.io.Text key, Iterable<DoubleWritable> values,Reducer<org.apache.hadoop.io.Text, DoubleWritable, org.apache.hadoop.io.Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            double val = 0;
            int count = 0;

            for(DoubleWritable w : values) {
                val += w.get();
                count++;
            }

            double avgDelay = (double) val/count;

            context.write(key, new DoubleWritable(avgDelay));
        }

    }


    public static void main( String[] args ){

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Monthly Delays trends!");
            job.setJarByClass(MonthlyDelaysTrend.class);
            job.setMapperClass(MonthlyDelaysTrendMapper.class);
            //job.setCombinerClass(MonthlyCancellationTrendReducer.class);
            job.setReducerClass(MonthlyDelaysTrendReducer.class);

            job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}