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
	private String screenName;
	private String createdDate;
	private int favoriteCount;
	private String countryName;

	public RealTimeTweet(String screenName, String createdDate, int favorite_count, String countryName) {
		super();
		this.screenName = screenName;
		this.createdDate = createdDate;
		this.favoriteCount = favorite_count;
		this.countryName = countryName;
	}

	@Override
	public String getPartitionColumnValue() {
		return "";
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
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

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}

}
