package control;

import java.io.*;
import java.util.Date;
import java.util.Properties;

/**
 * Created by maurizio on 19/04/17.
 */
public class AppConfigurator {

    public static final String FILENAME = "/Users/maurizio/Desktop/configuration.txt";

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
    public static long THRESHOLD = 1000; //millisecond
    public static long RANK_WINDOW_SIZE = 10; //seconds

    // avg Consumption
    public static long HOUR_CONS_WINDOW_SIZE = 10; //seconds
    public static long HOUR_CONS_WINDOW_SLIDE = 5; //seconds
    public static long DAY_CONS_WINDOW_SIZE = 72; //minutes
    public static long DAY_CONS_WINDOW_SLIDE = 12; //minuti
    public static long WEEK_CONS_WINDOW_SIZE = 504; //minuti
    public static long WEEK_CONS_WINDOW_SLIDE = 83; //minuti

    // median
    public static long MEDIAN_WINDOW_SIZE = 10; //secondi
    public static long MEDIAN_WINDOW_SLIDE = 5; //secondi



    public static void readConfiguration() {

        String result = "";
        
        try {
            Properties prop = new Properties();
            String propFileName = "/Users/maurizio/Desktop/config.properties";

            InputStream inputStream = new FileInputStream(propFileName);

            if(inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            Date time = new Date(System.currentTimeMillis());

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


            //result = "Company List = " + company1 + ", " + company2 + ", " + company3;
            //System.out.println(result + "\nProgram Ran on " + time + " by user=" + user);
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    
    }
}
