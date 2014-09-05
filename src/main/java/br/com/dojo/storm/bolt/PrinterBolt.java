package br.com.dojo.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import br.com.dojo.storm.twitter.Tweet;

public class PrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7897551985817409932L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		Tweet tweet = (Tweet) input.getValueByField("tweet");
		System.out.println(tweet);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
