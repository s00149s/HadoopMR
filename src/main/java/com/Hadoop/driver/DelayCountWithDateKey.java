package com.Hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.Hadoop.common.DateKey;
import com.Hadoop.common.DateKeyComparator;
import com.Hadoop.common.GroupKeyComparator;
import com.Hadoop.common.GroupKeyPartitioner;
import com.Hadoop.mapper.DelayCountMapperWithDateKey;
import com.Hadoop.mapper.DelayCountMapperWithMultipleOutputs;
import com.Hadoop.reducer.DelayCountReducerWithDateKey;
import com.Hadoop.reducer.DelayCountReducerWithMultipleOutputs;

public class DelayCountWithDateKey extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), args);
		System.out.println("MR-Job Result: " + res);

	}

	// run 메소드 오버라이드
	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage : DelayCountWithDateKey <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "DelayCountWithDateKey");

		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // Input 디렉터리 otherArgs
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // Output 디렉터리 otherArgs

		job.setJarByClass(DelayCountWithDateKey.class); // driver클래스 세팅
		job.setMapperClass(DelayCountMapperWithDateKey.class); // Mapper class 세팅
		job.setReducerClass(DelayCountReducerWithDateKey.class); // Reducer class 세팅

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);

		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);

		// 추가
		job.setPartitionerClass(GroupKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupKeyComparator.class);
		job.setSortComparatorClass(DateKeyComparator.class);
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		return 0;

	}
}
