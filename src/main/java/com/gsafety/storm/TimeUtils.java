package com.gsafety.storm;

import java.util.Calendar;

/**
 * Author: huangll
 * Written on 17/9/15.
 */
public class TimeUtils {

  public static long getTodayZeroTime() {
    Calendar c = Calendar.getInstance();
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTimeInMillis();
  }
}
