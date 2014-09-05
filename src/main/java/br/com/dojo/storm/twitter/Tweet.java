package br.com.dojo.storm.twitter;

import java.io.Serializable;
import java.util.Date;

public class Tweet implements Serializable{
	
	private Long id;
	private Date createdAt;
	private String screenName;
	private String message;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	public String getScreenName() {
		return screenName;
	}
	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	@Override
	public String toString() {
		return "Tweet [id=" + id + ", createdAt=" + createdAt + ", screenName=" + screenName + ", message=" + message + "]";
	}
}
