package in.ds256.Assignment2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test {

	public static void main(String[] args) {
		
		String timeStamp = "Thu Apr 06 15:24:15 +0000 2017";
		String TWITTER = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
		SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
		sf.setLenient(true);
		Date creationDate = null;
		try {
			creationDate = sf.parse(timeStamp);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/** Convert the time to unix epoch time **/
		Long milli = creationDate.getTime();
		System.out.println("The time in milliseconds is "+milli.toString());
	}
	
}
