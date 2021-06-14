package com.Hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{	// 라인번호, 출력내용, 출력단어, 갯수
	private final static IntWritable one = new IntWritable(1); 
	private Text word = new Text();
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)	//	value : 문장
			throws IOException, InterruptedException {
		
		StringTokenizer strToken = new StringTokenizer(value.toString());	//	StringTokenizer : 들어온 문자열을 tab이나 space로 단어로 쪼개줌 옵션 추가 가능
				//	value로 들어온 문자열을 단어로 분리
		while(strToken.hasMoreTokens()) {
			word.set(strToken.nextToken()); 	// nextToken 하나씩 가져올 때마다 단어 하나씩 가져오기
			context.write(word, one); 	// hadoop mapreduce쪽으로 보내기 => context로 hadoop과 통신
		}
	}
	
	

}
