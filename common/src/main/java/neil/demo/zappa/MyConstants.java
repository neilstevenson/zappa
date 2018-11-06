package neil.demo.zappa;

import java.time.LocalDate;

/**
 * <p>To avoid {@code D-R-Y}.
 * </p>
 */
public class MyConstants {

	/* ----------------- */
	/* -- Kafka stuff -- */
	/* ----------------- */
	public static final int KAFKA_PARTITION_COUNT = 3;

	public static final String BEAN_PRODUCER_PREFIX = "producer_";

	public static final String KAFKA_TOPIC_NAME_ACCOUNT = "account";
	public static final String KAFKA_TOPIC_NAME_FX = "fx";
	public static final String KAFKA_TOPIC_NAME_GPS = "gps";

	/* --------------------- */
	/* -- Hazelcast stuff -- */
	/* --------------------- */
	public static final String COMMAND_START = "start";
	public static final String COMMAND_STOP = "stop";
	
	public static final String IMAP_NAME_ACCOUNT = "account";
	public static final String IMAP_NAME_ALERT = "alert";
	public static final String IMAP_NAME_BTC_USD = "BTC/USD";
	public static final String IMAP_NAME_COMMAND = "command";
	public static final String IMAP_NAME_HAMLET = "hamlet";
	public static final String IMAP_NAME_JSESSIONID = "jsessionid";
	public static final String IMAP_NAME_POSITION = "position";
	public static final String IMAP_NAME_SPEED = "speed";
	public static final String IMAP_NAME_WORDS = "words";
	public static final String[] IMAP_NAMES = {
			IMAP_NAME_ACCOUNT,
			IMAP_NAME_ALERT,
			IMAP_NAME_BTC_USD,
			IMAP_NAME_COMMAND,
			IMAP_NAME_HAMLET,
			IMAP_NAME_JSESSIONID,
			IMAP_NAME_POSITION,
			IMAP_NAME_SPEED,
			IMAP_NAME_WORDS
	};

    public static final String ISET_NAME_JET_JOBS = "jet_jobs";
    public static final String[] ISET_NAMES = { 
                    ISET_NAME_JET_JOBS };

    public static final String ITOPIC_NAME_ALERT = "alert";
    public static final String[] ITOPIC_NAMES = new String[] { 
            ITOPIC_NAME_ALERT };
	
	public static final String JOB_NAME_ACCOUNT = "Account Materialisation";
	public static final String JOB_NAME_HEATHROW_EXPRESS_1 = "Heathrow Express ingest";
	public static final String JOB_NAME_HEATHROW_EXPRESS_2 = "Heathrow Express analysis";
	public static final String JOB_NAME_MOVING_AVERAGE = "Moving Average";
	public static final String JOB_NAME_WORD_COUNT= "Word Count";
	public static final String[] JOB_NAMES = new String[] { 
            JOB_NAME_ACCOUNT,
            JOB_NAME_HEATHROW_EXPRESS_1,
            JOB_NAME_HEATHROW_EXPRESS_2,
            JOB_NAME_MOVING_AVERAGE,
            JOB_NAME_WORD_COUNT
    };
	
	/* ----------------- */
	/* -- Chart stuff -- */
	/* ----------------- */
    public static final String KEY_CURRENT = "Current";
    public static final String KEY_50_POINT = "50 Point";
    public static final String KEY_200_POINT = "200 Point";

    public static final String PANEL_TITLE = "Analysis " + LocalDate.now();
    public static final String CHART_TITLE = "Bitcoin v US Dollar";
    public static final String CHART_X_AXIS = "Date";
    public static final String CHART_Y_AXIS = "US$";
    public static final String[] CHART_LINES = { 
                    KEY_CURRENT, KEY_50_POINT, KEY_200_POINT
                    };
}	
