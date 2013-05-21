package edu.bilkent.cs545.bolts;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.bilkent.cs545.utils.Pair;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CreatePairsBolt extends BaseBasicBolt{

	HashMap <String, Integer> uwords;
	
	public CreatePairsBolt(){
		uwords = new HashMap<String, Integer> ();	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		ArrayList<Pair> pairs = new ArrayList<Pair>();
		
		String tweet = (String) input.getValue(0);
		
		if(tweet.equals("kalender")){
			Pair p = new Pair("kalender", "kalender");
			pairs.add(p);
			//try {
			//This part is written to print out words.	
			System.out.println("number of words: " + uwords.size()) ;
				//LossyCountingBolt.printMap(uwords, "words.txt");
				//System.exit(0);
			//} catch (FileNotFoundException | UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			//}
			collector.emit(new Values(pairs));
		}
		String [] words = tweet.split(" ");
		if(words.length > 1){
			for(int i = 0; i<words.length; i++){
				for(int k = i+1; k<words.length; k++){
					Pair p = new Pair(words[i], words[k]);
					pairs.add(p);
				}

				
				//This Part is written to count words
//				if(!(uwords.containsKey(words[i]))){
//					uwords.put(words[i], 1);
//				}else{
//					int freq = uwords.get(words[i]);
//					freq = freq++;
//					uwords.put(words[i], freq);
//				}
			}
			collector.emit(new Values(pairs));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wordPairs"));
		
	}

}
