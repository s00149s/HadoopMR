package com.Hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator { // 복합키 상속
	protected DateKeyComparator() {
		super(DateKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;

		int cmp = k1.getYear().compareTo(k2.getYear());
		if (cmp != 0) {
			return cmp;
		}

		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1); // year끼리 비교했을때 같으면
		// 첫번째 꺼가 작으면 -1, 크면 1, 같으면 0

	}

}
