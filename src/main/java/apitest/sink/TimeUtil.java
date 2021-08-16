package apitest.sink;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class TimeUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TimeUtil.class);

    public static String timestamp2String(long timestamp) {
        return timestamp2String(timestamp, "yyyy-MM-dd HH:mm:ss");
    }

    public static String timestampSecond2String(long timestamp) {
        return timestamp2String(timestamp * 1000, "yyyy-MM-dd HH:mm:ss");
    }

    public static String timestamp2String(long timestamp, String pattern) {
        String tsStr = "";
        DateFormat sdf = new SimpleDateFormat(pattern);
        try {
            tsStr = sdf.format(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        return tsStr;
    }

    public static long string2TimestampSecond(String timeString) {
        return string2TimestampSecond(timeString, "yyyy-MM-dd HH:mm:ss");
    }

    public static long string2TimestampSecond(String timeString, String pattern) {
        Date date = new Date();
        //注意format的格式要与日期String的格式相匹配
        DateFormat sdf = new SimpleDateFormat(pattern);
        try {
            date = sdf.parse(timeString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date.getTime() / 1000;
    }

    public static long string2TimestampMs(String timeString, String pattern) {
        Date date = new Date();
        //注意format的格式要与日期String的格式相匹配
        DateFormat sdf = new SimpleDateFormat(pattern);
        try {
            date = sdf.parse(timeString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public static void getLastPeriodThroughputAggByHost(String shortHost, int minutes) {
        int period = 5;//表示5分钟
        //目前只支持按5分钟，10分钟，60分钟力度查询
        if (minutes == 5 || minutes == 10 || minutes == 60) {
            period = minutes;
        }
        int periodMilliSecond = period * 60 * 1000;

        long current = System.currentTimeMillis();
        String dateHourStr = TimeUtil.timestamp2String(current, "yyyy-MM-dd HH");
        String minuteStr = TimeUtil.timestamp2String(current, "mm");
        int currentMinute = Integer.valueOf(minuteStr);
        int mod = currentMinute / period;
        int minute = mod * period;

        String endTimeString = dateHourStr + ":" + minute + ":00";
        long endTimestamp = TimeUtil.string2TimestampSecond(endTimeString) * 1000;
        long startTimestamp = endTimestamp - periodMilliSecond;


        System.out.println(endTimeString);
        System.out.println(startTimestamp);
        System.out.println(endTimestamp);
    }

    //获取当前hour的timestamp，精确到毫秒
    public static long currentHourTimestamp() {
        long now = System.currentTimeMillis();
        long currentHourTimestamp = now - now % 3600000;
        return currentHourTimestamp;
    }

    //获取当前hour的timestamp，精确到秒
    public static long currentHourTimestampSecond() {
        long currentHourTimestamp = currentHourTimestamp() / 1000;
        return currentHourTimestamp;
    }

    public static void main(String[] args) {

        String access_id = System.getenv("GOPATH");
        System.out.println(access_id);
    }
}
