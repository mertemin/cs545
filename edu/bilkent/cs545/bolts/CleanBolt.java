package edu.bilkent.cs545.bolts;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.bilkent.cs545.utils.Stemmer;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CleanBolt extends BaseBasicBolt {

	String stopWordsRaw = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your";
	List<String> stopWords;

	Stemmer s;

	public CleanBolt() {
		stopWords = Arrays.asList(stopWordsRaw.split(","));
		s = new Stemmer();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// Read the raw tweet
		String tweet = input.getString(0);
		if(tweet.equals("kalender")){
			collector.emit(new Values("kalender"));
		}
		// Remove tags, hashes, links
		tweet = tweet.replaceAll("(#[^\\s]+)|(@[^\\s]+)|(http[s]?)[\\S]+", "").toLowerCase();

		// Replace everything except for word character with a
		// white space
		tweet = tweet.replaceAll("[^\\s\\w]|_", " ");

		// Create an array from the words
		String wordsArray[] = tweet.split("\\s+");
		List<String> bagOfWords = new ArrayList<String>();
		for (String word : wordsArray) {
			// Add stemmed word to bag-of-words if
			// it's length is greater than 1
			// and it is not in the stop words
			if ((word.length() > 1) && !(stopWords.contains(new String(word)))) {
				bagOfWords.add(s.stem(word));
			}
		}

		// Create the new tweet for debugging
		String newTweet = "";
		for (String word : bagOfWords) {
			newTweet += word + " ";
		}

		// For now let's emit a string rather than bag-of-words
		// Emitting bag-of-words would require a serializable data structure
		collector.emit(new Values(newTweet));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bag-of-words"));
	}

}
