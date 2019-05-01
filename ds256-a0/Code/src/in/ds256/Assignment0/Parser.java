package in.ds256.Assignment0;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.json.JSONArray;
import org.json.JSONObject;

public class Parser {

	JSONObject jObj;
	JSONObject userObj;
	

	public Parser(String input) {
		jObj = createJsonObject(input);
	}

	public Parser(JSONObject input) {
		jObj = input;
	}

	public Parser() {
		jObj = null;
		userObj = null;
	}

	private JSONObject createJsonObject(String input) {
		try {
			JSONObject jObj = new JSONObject(input);
			return jObj;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	/** This method is called from outside to create a json object **/
	public void setInputJson(String json) {
		
		try {
			jObj = createJsonObject(json);
			userObj = jObj.getJSONObject("user");
		}catch(Exception e){
			userObj = null;
		}

	}
	
	public void setInputJsonObject(JSONObject argJsonObject) {
		jObj = argJsonObject;
	}

	/** Possibly won't be using this much **/
	public String getTweet() {
		try {
			return jObj.getString("text");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}
	
	/**
	 * get the id_str in the JSON
	 * @return
	 */
	public String getTweetId() {
		try {
			if (jObj == null)
				return null;

			return jObj.getString("id_str");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	/**
	 * The id_str key of the user
	 * @return The owner of the tweet, the userId
	 */
	public String getUser() {
		try {
			if (userObj == null)
				return null;

			return userObj.getString("id_str");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	/** Mostly won't be using this since we can filter the deleted sweets **/
	public boolean checkIfDelete() {
		try {
			Object a = null;

			if (jObj == null)
				return true;

			a = jObj.get("delete");
			if (a != null)
				return true;

		} catch (Exception e) {
			System.out.println("JSR" + e.getMessage());
			return false;
		}
		return false;
	}

	/**
	 * Check if the tweet is retweeted ** return true if yes, false if no
	 */
	public boolean isRetweeted() {
		boolean result = false;

		try {
			if (jObj == null)
				return result;

			/** retweeted is the key **/
			result = jObj.getBoolean("retweeted");
			return result;

		} catch (Exception e) {
			System.out.println("JSR retweet" + e.getMessage());
			return result;
		}
	}

	
	/**
	 * Created time of the user
	 * @return
	 */
	public String getCreatedAt() {
		String createdAt = "";

		try {
			if (jObj == null)
				return createdAt;

			createdAt = jObj.getString("created_at");

			/**
			 * https://stackoverflow.com/questions/4521715/twitter-date-unparseable Wed Aug
			 * 27 13:08:45 +0000 2008
			 * https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
			 **/
			String TWITTER = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
			SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
			sf.setLenient(true);
			Date creationDate = sf.parse(createdAt);

			/** Convert the time to unix epoch time **/
			Long milli = creationDate.getTime();

			return milli.toString();

		} catch (Exception e) {
			System.out.println("This is getCreatedAt " + e.getMessage());
		}

		return createdAt;
	}
	
	/**
	 * Timestamp of the tweet
	 * @return
	 */
	public String getTimeStamp() {
		String timeStamp = "";

		try {
			if (userObj == null)
				return timeStamp;

			timeStamp = userObj.getString("created_at");

			/**
			 * https://stackoverflow.com/questions/4521715/twitter-date-unparseable Wed Aug
			 * 27 13:08:45 +0000 2008
			 * https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
			 **/
			String TWITTER = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
			SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
			sf.setLenient(true);
			Date creationDate = sf.parse(timeStamp);

			/** Convert the time to unix epoch time **/
			Long milli = creationDate.getTime();

			return milli.toString();

		} catch (Exception e) {
			System.out.println("This is getCreatedAt " + e.getMessage());
		}

		return timeStamp;
	}

	/**
	 * Get followers count returns a int value count if present , 0 else
	 **/
	public Integer getFollowersCount() {
		Integer followersCount = 0;

		try {

			if (userObj == null)
				return followersCount;

			followersCount = userObj.getInt("followers_count");
			System.out.println("The follower count is "+followersCount);
			return followersCount;

		} catch (Exception e) {
			System.out.println("In the function followers count ");
			e.printStackTrace();
			return followersCount;
		}

	}

	/**
	 * Get friends count returns a int value count if present , 0 else
	 **/
	public Integer getFriendsCount() {
		Integer friendsCount = 0;

		try {

			if (userObj == null)
				return friendsCount;

			friendsCount = userObj.getInt("friends_count");
			return friendsCount;

		} catch (Exception e) {
			System.out.println("In the function friends count ");
			e.printStackTrace();
			return friendsCount;
		}

	}
	
	/**
	 * Fetches a source tweet
	 * This attribute contains a representation of the original Tweet that was retweeted
	 * @return the JSON object of the original source tweet
	 */
	public JSONObject getRetweetJsonObject() {
		
		JSONObject myObj = null;
		
		try {
			if(jObj == null)
				return null;
			
			myObj = jObj.getJSONObject("retweeted_status");
			return myObj;
			
		}catch(Exception e) {
			System.out.println("Exception in the retweet function "+e.getMessage());
			return myObj;
		}
	}

	/**
	 * Returns the total number of hashtags in a tweet json, parsing the entities
	 * https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
	 **/
	public int getHashTags() {
		ArrayList<String> hashtags = new ArrayList<>();
		try {
			if (jObj == null)
				return 0;

			JSONObject entities = jObj.getJSONObject("entities");
			JSONArray hashtagsArray = entities.getJSONArray("hashtags");

			for (int i = 0; i < hashtagsArray.length(); i++) {
				hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
			}

			return hashtags.size();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return 0;
		}
	}

	/** Returns the text of hashtags **/
	public ArrayList<String> getHashTagArray() {
		ArrayList<String> hashtags = new ArrayList<>();
		try {
			if (jObj == null)
				return hashtags;

			JSONObject entities = jObj.getJSONObject("entities");
			JSONArray hashtagsArray = entities.getJSONArray("hashtags");
			for (int i = 0; i < hashtagsArray.length(); i++) {
				hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
			}
			return hashtags;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return hashtags;
		}
	}
}
