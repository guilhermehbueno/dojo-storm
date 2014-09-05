package br.com.dojo.storm.twitter;

import java.io.Serializable;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FakeTwitterStream implements Serializable{
	
	private final static String[] ACTORS = { "stefan", "dave", "pere", "nathan", "doug", "ted", "mary", "rose" };
	private final static String[] LOCATIONS = { "Spain", "USA", "Spain", "USA", "USA", "USA", "UK", "France" };
	private final static String[] SUBJECTS = { "berlin", "justinbieber", "hadoop", "life", "bigdata" };
	private final BlockingQueue<Tweet> queue = new ArrayBlockingQueue<Tweet>(1000);
	private long id=1;
	
	public FakeTwitterStream(int total) throws Exception {
		super();
		for (int i = 0; i < total; i++) {
			this.queue.put(generateRandomTweet());
		}
	}

	private Tweet generateRandomTweet(){
		Random random = new Random();
		Tweet tweet = new Tweet();
		tweet.setId(id++);
		tweet.setCreatedAt(new Date());
		tweet.setScreenName(ACTORS[random.nextInt(ACTORS.length)]);
		tweet.setMessage(LOCATIONS[random.nextInt(LOCATIONS.length)]+ " - " + SUBJECTS[random.nextInt(SUBJECTS.length)]);
		tweet.setCreatedAt(new Date());
		return tweet;
	}
	
	public Tweet getNextTweet(){
		return this.queue.poll();
	}
}
