package com.Hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.Hadoop.common.AirlinePerformanceParser;
import com.Hadoop.counter.DelayCounters;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		
		
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {	
				if(parser.getDepartureDelayTime() > 0) {	// Available 하고, 양수일 경우에만 mapreduce 작업을 할 것
					outputKey.set("D, " + parser.getYear() + "," + parser.getMonth());
					context.write(outputKey, outputValue);
				} else if(parser.getDepartureDelayTime() == 0) {
					context.getCounter(DelayCounters.scheduled_departure).increment(1);	// 정시에 출발하면 +1
					
				} else if(parser.getDepartureDelayTime() < 0) {	//	delaytime이 음수면 
					context.getCounter(DelayCounters.early_departure).increment(1);	// early_departure을 +1
					
				} else {
					context.getCounter(DelayCounters.not_available_departure).increment(1);
				}
			}
		}
		else if (workType.equals("arrival")) {
			if(parser.isArriveDelayAvailable()) {
				if (parser.getArriveDelayTime() > 0) {
					outputKey.set("A, " + parser.getYear() + "," + parser.getMonth());
					context.write(outputKey, outputValue);
				} else if(parser.getArriveDelayTime() == 0) {
					context.getCounter(DelayCounters.sceduled_arrival).increment(1);
				} else if(parser.getArriveDelayTime() < 0) {
					context.getCounter(DelayCounters.early_arrival).increment(1);
				} else {
					context.getCounter(DelayCounters.not_available_arrival).increment(1);
				}
					
			}
		}
			
	}
	}
