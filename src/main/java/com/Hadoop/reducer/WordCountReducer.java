package com.Hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{	// Reducer 상속받기
	// Reducer : 특정 단어 몇갠지 sum 
	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,	//	args를 보기좋게 key와 value로 바꿔주기
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value : values) {
			sum += value.get(); 	// sum에 들어오는 value들 누적하기
		}
		result.set(sum);
		context.write(key, result);
		// reduce method 끝
	}
	
	

}
