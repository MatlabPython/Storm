package com.gsafety.storm;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

public class LatencyMetrics {
	
	private static Logger logger = LoggerFactory.getLogger(LatencyMetrics.class);

	//数据处理延迟统计直方图
	public static Histogram latencyHistogram(){
		MetricRegistry metricRegistry = new MetricRegistry();
		Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
			.outputTo(logger)
			.convertRatesTo(TimeUnit.SECONDS)
			.convertDurationsTo(TimeUnit.MILLISECONDS)
			.build();
		reporter.start(10, TimeUnit.SECONDS);//10秒打印一次日志
		
		Histogram histogram = metricRegistry.histogram(MetricRegistry.name(LatencyMetrics.class, "process-latancy"));
		return histogram;
	}
	
	//测试统计功能
	public static void main(String[] args) throws InterruptedException {
		Histogram histogram = latencyHistogram();
		Random random = new Random();
		while(true){
			int i = random.nextInt(100);
			histogram.update(i);
			Thread.sleep(1000);
		}
	}
}
