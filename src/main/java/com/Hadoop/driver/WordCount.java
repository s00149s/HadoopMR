package com.Hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // 입력포멧
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; // 출력포멧

import com.Hadoop.mapper.WordCountMapper;
import com.Hadoop.reducer.WordCountReducer;

public class WordCount {
	public static void main(String[] args) throws Exception { // 예외처리 생략(try, catch 안하려고) -> exception 발생하면 던지기
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.out.println("Usage: WordCount <input> <output>"); // length가 2개가 아니면 예외처리
			System.exit(2); // 숫자 0 빼고 아무거나
		}

		Job job = Job.getInstance(conf, "WordCount"); // job 생성, mapreduce에 job의 이름을 "WordCount"로 부여

		job.setJarByClass(WordCount.class); // driver클래스 세팅
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); // Input 디렉터리
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output 디렉터리

		job.waitForCompletion(true);

		// 기본적인 세팅

	}
}
