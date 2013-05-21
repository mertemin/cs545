package edu.bilkent.cs545.utils;

import java.io.Serializable;
import java.util.HashMap;

public class Pair implements Serializable{

	
	String forwards;
	
	String first;
	String second;
	private static final long serialVersionUID = 1L;

	public Pair(String first, String second){
		this.first = first;
		this.second = second;
		if(first.compareTo(second)<= 0 ){
			forwards = first + second;
		}else{
			forwards = second + first;
		}
		
	}

	public String getForwards() {
		return forwards;
	}

	public void setForwards(String forwards) {
		this.forwards = forwards;
	}


	public boolean check( HashMap<String, Integer> map){
		
		if(map.containsKey(this.forwards)){
			return true;
		}else{
			return false;
		}
		
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}
	
}
