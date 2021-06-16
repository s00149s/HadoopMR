package com.Hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class DateKey implements WritableComparable<DateKey> {
	private String year;
	private Integer month;

	public DateKey() { // bin 생성자

	}

	// 우클릭 > Source > Generate fields
	public DateKey(String year, String montrh) {
		this.year = year;
		this.month = month;
	}

	// 두 항목에 대하여 getter와 setter 만들어주기
	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	// Override/implement Methods : 기본 3개에 toString()까지 override

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, year);
		out.writeInt(month); // 하둡의 write 메소드 형식 구현

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = WritableUtils.readString(in);
		month = in.readInt();

	}

	// DateKey에 CompareTo 구현
	@Override
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year); // 자바 String 클래스의 compareTo 메서드로 문자열 year끼리 비교
		if (result == 0) { // 동일하면 0, 뒤쪽이 적으면 음수, 앞쪽이 적으면 양수
			result = month.compareTo(key.month);
		}
		return 0;
	}

	@Override
	public String toString() {
		// 연도, 월 -> string으로 만들어줌
		// +로 이어주지 않고, append로 연결해줌 -> 문자열이 길어지면 메모리 효율이 급격히 나빠져 StringBuilder 쓰는 것이 유리
		return new StringBuilder().append(year).append(",").append(month).toString();
	}

}
