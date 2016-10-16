package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

// Define a class implementing the Function<String, Boolean> interface
@SuppressWarnings("serial")
public class StartFilter implements Function<String, Boolean> {
	// Implement the call method
	// The call method receives one element (one string) 
	// and returns true if the element contains the word google.
	// Otherwise, it returns false.

	private String startKey;
	public StartFilter(String key) {
		this.startKey = key;
	}
	public Boolean call(String logLine) { 
		return logLine.toLowerCase().startsWith(startKey);
	}
}
