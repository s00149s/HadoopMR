package com.Hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.Hadoop.mapper.ArrivalDelayCountMapper;
import com.Hadoop.reducer.DelayCountReducer;

public class ArrivalDelayCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.out.println("Usage : ArrivalDelayCount <input> <output>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "ArrivalDelayCount");

		job.setJarByClass(ArrivalDelayCount.class); // driver클래스 세팅
		job.setMapperClass(ArrivalDelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); // Input 디렉터리
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output 디렉터리

		job.waitForCompletion(true);

	}

}
