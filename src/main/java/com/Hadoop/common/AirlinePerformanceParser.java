package com.Hadoop.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {	//	 객체 생성
	private int year;
	private int month;
	private int day;
	
	private int arriveDelayTime = 0;	//	도착지연시간
	private int departureDelayTime = 0;	//	출발지연시간
	private int distance = 0;	// 거리
	
	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;	// departureDelayTime 비어있을 때 사용
	private boolean distanceAvailable = true;
	
	private String uniqueCarrier;
	// 데이터에서 한 줄씩 객체 뽑아내기
	
	public AirlinePerformanceParser(Text text) {	// 생성자
		try {
			String[] columns = text.toString().split(","); 	// ,로 구분해서 String을 colum 배열에 넣기
			
			year = Integer.parseInt(columns[0]);	// 컬럼 순서 직접 확인
			month = Integer.parseInt(columns[1]);
			day = Integer.parseInt(columns[2]);		
			uniqueCarrier = columns[5];
			
			if(!columns[16].equals("")) {	//	16번째 컬럼(dep_delay)이 비어있지 않으면
				departureDelayTime = (int)Float.parseFloat(columns[16]);
			} else {
				departureDelayAvailable = false;
			}
			if(!columns[26].equals("")) {	//	26번째 컬럼(ari_delay)이 비어있지 않으면
				arriveDelayTime = (int)Float.parseFloat(columns[26]);
			} else {
				arriveDelayAvailable = false;
			}
			if(!columns[37].equals("")) {	//	37번째 컬럼(dis)이 비어있지 않으면
				distance = (int)Float.parseFloat(columns[37]);
			} else {
				distanceAvailable = false;
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getDay() {
		return day;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	
	
	
}
