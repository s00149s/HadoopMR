package com.Hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.Hadoop.common.AirlinePerformanceParser;
import com.Hadoop.common.DateKey;
import com.Hadoop.counter.DelayCounters;

public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {

	private final static IntWritable outputValue = new IntWritable();
	private DateKey outputKey = new DateKey();

	// 라인번호, 라인내용, 출력,
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {

		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) { // Available 하고, 양수일 경우에만 mapreduce 작업을 할 것
				outputKey.setYear("D," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1); // 정시에 출발하면 +1

			} else if (parser.getDepartureDelayTime() < 0) { // delaytime이 음수면
				context.getCounter(DelayCounters.early_departure).increment(1); // early_departure을 +1

			} else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}

		}
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				outputKey.setYear("A," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputValue);
			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.sceduled_arrival).increment(1);
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			} else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}

		}
	}

}
