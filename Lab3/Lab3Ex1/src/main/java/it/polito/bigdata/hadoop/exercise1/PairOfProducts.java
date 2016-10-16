package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.io.Text;

public class PairOfProducts implements Comparable<PairOfProducts> {
	private Text pair;
	private Long count;
	
	public PairOfProducts(Text pair, Long count) {
		this.pair = pair;
		this.count = count;
	}
	public Text getPair() {
		return pair;
	}
	public void setPair(Text pair) {
		this.pair = pair;
	}
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
	@Override
	public int compareTo(PairOfProducts other) {
		// The compareTo must:
		// order values by natural ordering
		// distinguish unique values (<pair,count> pairs)
		int a = this.count.compareTo(other.getCount());
		if (a != 0) {
			return a; //order by count
		} else {
			return this.pair.compareTo(other.getPair()); //order by pair
		}
	}
}
