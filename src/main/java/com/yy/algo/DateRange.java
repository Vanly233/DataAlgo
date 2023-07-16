package com.yy.algo;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateRange extends UDF {
    Map<String, String> formatMap = new HashMap<>();
    SimpleDateFormat formatter = new SimpleDateFormat();

    Map<String, Integer> calendarMap = new HashMap<>();

    Integer step = 1;

    String dateFreq;

    Integer calendarFreq;

    public DateRange(){
        setFormatMap();
        setCalendarMap();
    }

    private void setFormatMap(){
        formatMap.put("D", "yyyy-MM-dd");
        formatMap.put("M", "yyyy-MM");
        formatMap.put("Y", "yyyy");
        formatMap.put("H", "yyyy-MM-dd HH");
        formatMap.put("MIN", "yyyy-MM-dd HH:mm");
        formatMap.put("SEC", "yyyy-MM-dd HH:mm:ss");
    }

    private void setCalendarMap(){
        calendarMap.put("D", Calendar.DAY_OF_MONTH);
        calendarMap.put("M", Calendar.MONTH);
        calendarMap.put("Y", Calendar.YEAR);
        calendarMap.put("H", Calendar.HOUR_OF_DAY);
        calendarMap.put("MIN", Calendar.MINUTE);
        calendarMap.put("SEC", Calendar.SECOND);
    }

    private void setFormatter(String freq, String format){
        String[] freqArr = parseFreq(freq);
        step = Integer.valueOf(freqArr[0]);
        dateFreq = freqArr[1];
        calendarFreq = calendarMap.get(dateFreq);
        if(format!=null){
            formatter.applyPattern(format);
        }else{
            formatter.applyPattern(formatMap.get(dateFreq));
        }
    }

    public String[] parseFreq(String freq){
        String pattern = "(\\d+) ?(D|Y|H|MIN|SEC|M)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(freq.toUpperCase());
        if(m.find()){
            return new String[]{m.group(1), m.group(2)};
        }else{
            throw new IllegalArgumentException("Unrecognized freq format: " + freq);
        }
    }

    private void dateAdd(Calendar calendar, int step) {
        calendar.add(calendarFreq, step);
    }

    public List<String> evaluate(String startDate, Integer periods, String freq) throws ParseException {
        return evaluate(startDate, periods, freq, null);
    }

    public List<String> evaluate(String startDate, String endDate, String freq) throws ParseException{
        return evaluate(startDate, endDate, freq, null);
    }

    /**
     *
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param freq 频率
     * @return [startDate, endDate]间每隔 freq 这个频率给出一个值
     */
    public List<String> evaluate(String startDate, String endDate, String freq, String format) throws ParseException{
        if (startDate == null || endDate == null || freq == null) {
            throw new IllegalArgumentException("Invalid arguments: startDate, endDate, freq cannot be empty.");
        }
        setFormatter(freq, format);
        Date start = formatter.parse(startDate);
        Date end = formatter.parse(endDate);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        ArrayList<String> result = new ArrayList<>();
        while(!calendar.getTime().after(end)){
            result.add(formatter.format(calendar.getTime()));
            dateAdd(calendar, step);
        }
        return result;
    }

    public List<String> evaluate(String startDate, Integer periods, String freq, String format) throws ParseException {
        if (startDate == null || freq == null) {
            throw new IllegalArgumentException("Invalid arguments: startDate or freq cannot be empty.");
        }
        if (periods.equals(0)) {
            throw new IllegalArgumentException("Invalid arguments: periods cannot equal to 0.");
        }

        setFormatter(freq, format);
        Date start = formatter.parse(startDate);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        ArrayList<String> result = new ArrayList<>();
        int sign = periods>0?1:-1;
        for(int i=0;i<(periods*sign);i++){
            result.add(formatter.format(calendar.getTime()));
            dateAdd(calendar, sign*step);
        }
        return result;
    }

    public static void main(String[] args) throws ParseException {
//        DateRange dr = new DateRange();
//        System.out.println(dr.evaluate("2023-01-01 00:00:00",48,"30 MIN"));
//        System.out.println(dr.evaluate("2023-01-01 00:00:00",48,"30 MIN", "yyyy-MM-dd HH:mm:ss"));
//        System.out.println(dr.evaluate("2023-01-01 00:00:00","2023-01-02 00:00:00","30 MIN"));
//        System.out.println(dr.evaluate("2023-01-01 00:00:00","2023-01-02 00:00:00","30 MIN", "yyyy-MM-dd HH:mm:ss"));
    }

}