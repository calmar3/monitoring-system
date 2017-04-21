package control;

import java.io.*;
import java.util.Properties;

/**
 * Created by maurizio on 19/04/17.
 */
public class AppConfigurator {

    public static final String FILENAME = "/Users/maurizio/Desktop/config.properties";

    public static String LAMP_DATA_TOPIC = "lamp_data";
    public static String RANK_TOPIC = "rank";

    public static String WARNING_HOUR_TOPIC = "warning_hour";
    public static String WARNING_DAY_TOPIC = "warning_day";
    public static String WARNING_WEEK_TOPIC = "warning_week";
    public static String WARNING_STATE = "warning_state";

    public static String HOUR_LAMP_CONS = "hour_lamp_cons";
    public static String DAY_LAMP_CONS = "day_lamp_cons";
    public static String WEEK_LAMP_CONS = "week_lamp_cons";

    public static String HOUR_STREET_CONS = "hour_street_cons";
    public static String DAY_STREET_CONS = "day_street_cons";
    public static String WEEK_STREET_CONS = "week_street_cons";

    public static String HOUR_CITY_CONS = "hour_city_cons";
    public static String DAY_CITY_CONS = "day_city_cons";
    public static String WEEK_CITY_CONS = "week_city_cons";

    public static String MEDIAN_TOPIC = "median";


    // ranking
    public static int MAX_RANK_SIZE = 3;
    public static long THRESHOLD = 1000; //milliseconds
    public static long RANK_WINDOW_SIZE = 10; //seconds

    // avg
    public static long HOUR_CONS_WINDOW_SIZE = 10; //seconds
    public static long HOUR_CONS_WINDOW_SLIDE = 5; //seconds
    public static long DAY_CONS_WINDOW_SIZE = 4320; //seconds
    public static long DAY_CONS_WINDOW_SLIDE = 720; //seconds
    public static long WEEK_CONS_WINDOW_SIZE = 30240; //seconds
    public static long WEEK_CONS_WINDOW_SLIDE = 4980; //seconds

    // median
    public static long MEDIAN_WINDOW_SIZE = 10; //seconds
    public static long MEDIAN_WINDOW_SLIDE = 5; //seconds


    /*
	// ranking
	private static final int MAX_RANK_SIZE = 3;
	private static final long THRESHOLD = 1000;
	private static final long RANK_WINDOW_SIZE = 10; //seconds

	// avg Consumption
	private static final long HOUR_CONS_WINDOW_SIZE = 3600; //seconds 1 hour
	private static final long HOUR_CONS_WINDOW_SLIDE = 600; //seconds 10 minutes
	private static final long DAY_CONS_WINDOW_SIZE = 1440; //minutes 24 hours
	private static final long DAY_CONS_WINDOW_SLIDE = 240; //minutes 4 hours
	private static final long WEEK_CONS_WINDOW_SIZE = 10080; //minutes 7 days
	private static final long WEEK_CONS_WINDOW_SLIDE = 1440; //minutes 1 day

	// median
	private static final long MEDIAN_WINDOW_SIZE = 3600; //seconds 1 hour
	private static final long MEDIAN_WINDOW_SLIDE = 600; //seconds 10 minutes
*/



    public void readConfiguration() {

        try {
            Properties prop = new Properties();

            FileInputStream inputStream = new FileInputStream(FILENAME);

            prop.load(inputStream);

            // set topic
            LAMP_DATA_TOPIC = prop.getProperty("LAMP_DATA_TOPIC");
            RANK_TOPIC = prop.getProperty("RANK_TOPIC");
            WARNING_HOUR_TOPIC = prop.getProperty("WARNING_HOUR_TOPIC");
            WARNING_DAY_TOPIC = prop.getProperty("WARNING_DAY_TOPIC");
            WARNING_WEEK_TOPIC = prop.getProperty("WARNING_WEEK_TOPIC");
            WARNING_STATE = prop.getProperty("WARNING_STATE");
            HOUR_LAMP_CONS = prop.getProperty("HOUR_LAMP_CONS");
            DAY_LAMP_CONS = prop.getProperty("DAY_LAMP_CONS");
            WEEK_LAMP_CONS = prop.getProperty("WEEK_LAMP_CONS");
            HOUR_STREET_CONS = prop.getProperty("HOUR_STREET_CONS");
            DAY_STREET_CONS = prop.getProperty("DAY_STREET_CONS");
            WEEK_STREET_CONS = prop.getProperty("WEEK_STREET_CONS");
            HOUR_CITY_CONS = prop.getProperty("HOUR_CITY_CONS");
            DAY_CITY_CONS = prop.getProperty("DAY_CITY_CONS");
            WEEK_CITY_CONS = prop.getProperty("WEEK_CITY_CONS");
            MEDIAN_TOPIC = prop.getProperty("MEDIAN_TOPIC");

            // set param for rank
            MAX_RANK_SIZE = Integer.parseInt(prop.getProperty("MAX_RANK_SIZE"));
            THRESHOLD = Long.parseLong(prop.getProperty("THRESHOLD"));
            RANK_WINDOW_SIZE = Long.parseLong(prop.getProperty("RANK_WINDOW_SIZE"));

            // set param for avg
            HOUR_CONS_WINDOW_SIZE = Long.parseLong(prop.getProperty("HOUR_CONS_WINDOW_SIZE"));
            HOUR_CONS_WINDOW_SLIDE = Long.parseLong(prop.getProperty("HOUR_CONS_WINDOW_SLIDE"));
            DAY_CONS_WINDOW_SIZE = Long.parseLong(prop.getProperty("DAY_CONS_WINDOW_SIZE"));
            DAY_CONS_WINDOW_SLIDE = Long.parseLong(prop.getProperty("DAY_CONS_WINDOW_SLIDE"));
            WEEK_CONS_WINDOW_SIZE = Long.parseLong(prop.getProperty("WEEK_CONS_WINDOW_SIZE"));
            WEEK_CONS_WINDOW_SLIDE = Long.parseLong(prop.getProperty("WEEK_CONS_WINDOW_SLIDE"));

            // set param for median
            MEDIAN_WINDOW_SIZE = Long.parseLong(prop.getProperty("MEDIAN_WINDOW_SIZE"));
            MEDIAN_WINDOW_SLIDE = Long.parseLong(prop.getProperty("MEDIAN_WINDOW_SLIDE"));

            /*System.out.println(LAMP_DATA_TOPIC + "\n"
                                + RANK_TOPIC + "\n"
                                + WARNING_HOUR_TOPIC + "\n"
                                + WARNING_DAY_TOPIC + "\n"
                                + WARNING_WEEK_TOPIC + "\n"
                                + WARNING_STATE + "\n"
                                + HOUR_LAMP_CONS + "\n"
                                + DAY_LAMP_CONS + "\n"
                                + WEEK_LAMP_CONS + "\n"
                                + HOUR_STREET_CONS + "\n"
                                + DAY_STREET_CONS + "\n"
                                + WEEK_STREET_CONS + "\n"
                                + HOUR_CITY_CONS + "\n"
                                + DAY_CITY_CONS + "\n"
                                + WEEK_CITY_CONS + "\n"
                                + MEDIAN_TOPIC + "\n"
                                + MAX_RANK_SIZE + "\n"
                                + THRESHOLD + "\n"
                                + RANK_WINDOW_SIZE + "\n"
                                + HOUR_CONS_WINDOW_SIZE + "\n"
                                + HOUR_CONS_WINDOW_SLIDE + "\n"
                                + DAY_CONS_WINDOW_SIZE + "\n"
                                + DAY_CONS_WINDOW_SLIDE + "\n"
                                + WEEK_CONS_WINDOW_SIZE + "\n"
                                + WEEK_CONS_WINDOW_SLIDE + "\n"
                                + MEDIAN_WINDOW_SIZE + "\n"
                                + MEDIAN_WINDOW_SLIDE + "\n");*/

        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
