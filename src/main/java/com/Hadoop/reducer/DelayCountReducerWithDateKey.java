package com.Hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.Hadoop.common.DateKey;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();

	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {

		mos = new MultipleOutputs<DateKey, IntWritable>(context);

	}

	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] columns = key.getYear().split(",");

		int sum = 0;
		Integer bMonth = key.getMonth(); // 월 비교 -> bmonth에 sorting되어 같은 month를 축적
		// bMonth는 항상 최신으로 갱신됨

		if (columns[0].equals("D")) {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) { // bMonth와 Month가 다르면
					// D or A , 연도 -> columns[0] == D or A
					// 여태까지 쌓인 데이터와 다를 경우, 여태까지 쌓인 데이터를 write 해주고 새로 쌓기 위한 코드
					result.set(sum); // 쌓인 데이터 set
					outputKey.setYear(key.getYear().substring(2)); // 실제 연도는 2번째 컬럼으로, 쌓인 연도 set
					outputKey.setMonth(bMonth); // sorting되어 축적된 month를 set하기
					mos.write("departure", outputKey, result); // 축적된 데이터 써주기
					sum = 0; // 다시 초기화 하고 새로운 Month 쌓기 준비
				}

				sum += value.get(); // bMonth가 Month와 같으면
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) { // bMonth와 Month가 같으면 => 계속 축적해주기(결과 출력x)
				result.set(sum);
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				mos.write("departure", outputKey, result);
			}

		} else {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(key.getYear().substring(2));
					outputKey.setMonth(bMonth);
					mos.write("arrival", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) {
				result.set(sum);
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				mos.write("arrival", outputKey, result);
			}
		}

	}

	@Override
	protected void cleanup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// mos를 계속 실행시키면 누적되어 메모리 누수 생김
		// 맨 마지막에 실행, 메모리 누수 방지
		mos.close();
	}

}
