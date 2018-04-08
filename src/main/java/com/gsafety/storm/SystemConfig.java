package com.gsafety.storm;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class SystemConfig {
  private static Properties props;

  static {
    props = new Properties();
    try {
      InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("system.properties");
      props.load(in);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String get(String key) {
    return props.getProperty(key);
  }

  public static String get(String key, String defaultValue) {
    String v = props.getProperty(key);
    if (v != null && !"".equals(v)) {
      return v;
    } else {
      return defaultValue;
    }
  }

  public static int getInt(String key) {
    String value = props.getProperty(key);
    return Integer.parseInt(value);
  }

  public static int getInt(String key, int defaultValue) {
    String value = props.getProperty(key);
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

}
