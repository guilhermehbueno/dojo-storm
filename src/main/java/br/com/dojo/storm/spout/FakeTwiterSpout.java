package br.com.dojo.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import br.com.dojo.storm.twitter.FakeTwitterStream;
import br.com.dojo.storm.twitter.Tweet;

public class FakeTwiterSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = -5920502143243376951L;
	private FakeTwitterStream fakeTwitterStream;
	private SpoutOutputCollector collector;
	
	public FakeTwiterSpout() throws Exception {
		super();
		this.fakeTwitterStream = new FakeTwitterStream(1000);
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}

	public void nextTuple() {
		Tweet tweet = this.fakeTwitterStream.getNextTweet();
		if(tweet!=null){
			this.collector.emit(new Values(tweet));
		}else{
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
