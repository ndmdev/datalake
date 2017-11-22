/**
 * 
 */
package com.nd.datalake.stream.event.custom;

import com.nd.datalake.stream.event.RealtimeEvent;

/**
 * @author krish
 *
 */
public class RealTimeTweet implements RealtimeEvent {
	private String userA;
	private String createdDate;
	private int favoriteCount;
	private String userB;

	public RealTimeTweet(String screenName, String createdDate, int favorite_count, String countryName) {
		super();
		this.userA = screenName;
		this.createdDate = createdDate;
		this.favoriteCount = favorite_count;
		this.userB = countryName;
	}

	@Override
	public String getPartitionColumnValue() {
		return "";
	}

	public String getUserA() {
		return userA;
	}

	public void setUserA(String userA) {
		this.userA = userA;
	}

	public String getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(String createdDate) {
		this.createdDate = createdDate;
	}

	public int getFavoriteCount() {
		return favoriteCount;
	}

	public void setFavoriteCount(int favoriteCount) {
		this.favoriteCount = favoriteCount;
	}

	public String getUserB() {
		return userB;
	}

	public void setUserB(String userB) {
		this.userB = userB;
	}

}
