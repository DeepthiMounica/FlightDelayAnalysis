

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirlineWithMostCancellation {

    public static class MonthlyCancellationTrendMapper extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, IntWritable>{

        @Override
        protected void map(Object key, org.apache.hadoop.io.Text value, Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            if (!value.toString().contains("arr_del15")) {

                String[] input = value.toString().split(",");

                if(input.length >7 && !input[7].equalsIgnoreCase("0") && !input[15].equalsIgnoreCase("0")){

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

                        int minutes = Integer.parseInt(input[28]);

                        context.write(month, new IntWritable(minutes));

                    } catch (Exception ex) {
                        System.out.println("Skip..");
                        //UniqueCarrier.set(input[2]);
                        //context.write(UniqueCarrier, new AverageBean(0, 0));
                    }
                }
            }
        }

    }


    public static class MonthlyCancellationTrendReducer extends Reducer<org.apache.hadoop.io.Text, IntWritable, org.apache.hadoop.io.Text, IntWritable> {

        @Override
        protected void reduce(org.apache.hadoop.io.Text key, Iterable<IntWritable> values,Reducer<org.apache.hadoop.io.Text, IntWritable, org.apache.hadoop.io.Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            int val = 0;

            for(IntWritable w : values) val += w.get();

            context.write(key, new IntWritable(val));
        }

    }


    public static void main( String[] args ){

        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "Monthly Cancellation trends!");
            job.setJarByClass(MonthlyDelaysTrend.class);
            job.setMapperClass(MonthlyCancellationTrendMapper.class);
            job.setCombinerClass(MonthlyCancellationTrendReducer.class);
            job.setReducerClass(MonthlyCancellationTrendReducer.class);

            job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}