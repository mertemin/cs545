package edu.bilkent.cs545.utils;

import java.io.Serializable;
import java.util.HashMap;

public class Pair implements Serializable{

	
	String forwards;
	String backwards;
	private static final long serialVersionUID = 1L;

	public Pair(String first, String second){
		forwards = first + second;
		backwards = second + first;
	}

	public String getForwards() {
		return forwards;
	}

	public void setForwards(String forwards) {
		this.forwards = forwards;
	}

	public String getBackwards() {
		return backwards;
	}

	public void setBackwards(String backwards) {
		this.backwards = backwards;
	}
	

	public String check( HashMap<String, Integer> map){
		
		if(map.containsKey(this.forwards)){
			return this.forwards;
		}else if(map.containsKey(this.backwards)){
			return this.backwards;
		}else{
			return null;
		}
		
		
	}
	
}
