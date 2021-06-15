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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.Hadoop.mapper.DelayCountMapper;
import com.Hadoop.reducer.DelayCountReducer;

public class DelayCountWithCounter extends Configured implements Tool{
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new DelayCountWithCounter(), args);	// 결과가 정상인지 아닌지 확인
		System.out.println("MR-Job Result: " + res);
	}

	@Override
	public int run(String[] args) throws Exception {	// 추상메서드 run override
		// GenericOtionParser에서 제공하는 파라미터를 제외한 나머지 파라미터 가져오기
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("Usage : DelayCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(), "DelayCount");	//	generic에서는 getConf로 가져옴
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // Input 디렉터리 otherArgs
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // Output 디렉터리 otherArgs
		
		job.setJarByClass(DelayCount.class); // driver클래스 세팅
		job.setMapperClass(DelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		
		
		
		return 0;
	}

}
