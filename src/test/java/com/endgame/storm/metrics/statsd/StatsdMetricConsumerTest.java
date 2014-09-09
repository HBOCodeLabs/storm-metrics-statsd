/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Endgame Inc.
 *
 */

package com.endgame.storm.metrics.statsd;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer.DataPoint;
import backtype.storm.metric.api.IMetricsConsumer.TaskInfo;

import com.endgame.storm.metrics.statsd.StatsdMetricConsumer.Metric;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * @author Jason Trost
 */
public class StatsdMetricConsumerTest extends TestCase {

	StatsdMetricConsumer undertest;

	@Override
	protected void setUp() throws Exception {
		undertest = new StatsdMetricConsumer();
	}

	public void testParseConfig() {
		assertNull(undertest.statsdHost);
		assertEquals("storm.metrics.", undertest.statsdPrefix);
		assertEquals(8125, undertest.statsdPort);
		assertNull(undertest.topologyName);

		Map conf = new HashMap();
		conf.put(StatsdMetricConsumer.STATSD_HOST, "localhost");
		// Test that storm/clojure would magically convert int to Long
		conf.put(StatsdMetricConsumer.STATSD_PORT, 5555l);
		conf.put(StatsdMetricConsumer.STATSD_PREFIX, "my.statsd.prefix");
		conf.put(Config.TOPOLOGY_NAME, "myTopologyName");

		undertest.parseConfig(conf);

		assertEquals("localhost", undertest.statsdHost);
		assertEquals("my.statsd.prefix.", undertest.statsdPrefix);
		assertEquals(5555, undertest.statsdPort);
		assertEquals("myTopologyName", undertest.topologyName);

		conf.put(StatsdMetricConsumer.STATSD_PORT, "4444");
		undertest.parseConfig(conf);
		assertEquals(4444, undertest.statsdPort);

		conf.put(StatsdMetricConsumer.STATSD_HOST, null);
		conf.put(StatsdMetricConsumer.STATSD_PREFIX, null);
		conf.put(StatsdMetricConsumer.STATSD_PORT, null);
		undertest.parseConfig(conf);
		assertEquals(4444, undertest.statsdPort);
		assertEquals("localhost", undertest.statsdHost);
		assertEquals("my.statsd.prefix.", undertest.statsdPrefix);
	}

	public void testCleanString() {
		assertEquals("test", undertest.clean("test"));
		assertEquals("test_name", undertest.clean("test/name"));
		assertEquals("test_name", undertest.clean("test:name"));
	}

	public void testCamcelCase() {
		assertEquals("B", undertest.toCamelCase("b"));
		assertEquals("Burger", undertest.toCamelCase("burger"));
		assertEquals("B", undertest.toCamelCase("B"));
		assertEquals("B", undertest.toCamelCase("__B"));
		assertEquals("B", undertest.toCamelCase("B__"));
		assertEquals("BK", undertest.toCamelCase("B_K"));
		assertEquals("", undertest.toCamelCase("__"));
		assertEquals("BurgerKing", undertest.toCamelCase("BURGER_KING"));
	}

	public void testPrepare() {
		assertNull(undertest.statsdHost);
		assertEquals("storm.metrics.", undertest.statsdPrefix);
		assertEquals(8125, undertest.statsdPort);
		assertNull(undertest.topologyName);
		assertNull(undertest.statsd);

		Map stormConf = new HashMap();
		stormConf.put(Config.TOPOLOGY_NAME, "myTopologyName");

		Map registrationArgument = new HashMap();
		registrationArgument.put(StatsdMetricConsumer.STATSD_HOST, "localhost");
		registrationArgument.put(StatsdMetricConsumer.STATSD_PORT, 5555);
		registrationArgument.put(StatsdMetricConsumer.STATSD_PREFIX,
				"my.statsd.prefix");

		undertest.prepare(stormConf, registrationArgument, null, null);

		assertEquals("localhost", undertest.statsdHost);
		assertEquals("my.statsd.prefix.", undertest.statsdPrefix);
		assertEquals(5555, undertest.statsdPort);
		assertEquals("myTopologyName", undertest.topologyName);
		assertNotNull(undertest.statsd);
	}

	public void testDataPointsToMetrics() {
		TaskInfo taskInfo = new TaskInfo("host.1", 6701, "MY_BOLT_7", 12,
				123456789000L, 60);
		List<DataPoint> dataPoints = new LinkedList<>();

		dataPoints.add(new DataPoint("__system.int", 22));
		dataPoints.add(new DataPoint("my.elapsed.int", 22));
		dataPoints.add(new DataPoint("my.gauge.int", 57));
		dataPoints.add(new DataPoint("my.int.gauge", 58));
		dataPoints.add(new DataPoint("my.int.elapsed", 59));
		dataPoints.add(new DataPoint("my.long", 57L));
		dataPoints.add(new DataPoint("my/float", 222f));
		dataPoints.add(new DataPoint("my_double", 56.0d));
		dataPoints.add(new DataPoint("ignored", "not a num"));
		dataPoints.add(new DataPoint("points", ImmutableMap
				.<String, Object> of("elapsed", 3321,
					"gauge", 1234,
					"count", 123,
					"time", 2342234,
					"ignored", "not a num")));

		undertest.topologyName = "testTop";
		undertest.statsdPrefix = "testPrefix";

		// topology and prefix are used when creating statsd, and statsd client
		// handles adding them
		// they should not show up here

		List<Metric> expected = ImmutableList.<Metric> of(new Metric(
				"MyBolt7.my.elapsed.int", 22), new Metric(
				"host_1:6701.MyBolt7.my.elapsed.int", 22), new Metric(
				"MyBolt7.my.gauge.int", 57), new Metric(
				"host_1:6701.MyBolt7.my.gauge.int", 57), new Metric(
				"MyBolt7.my.int.gauge", 58), new Metric(
				"host_1:6701.MyBolt7.my.int.gauge", 58), new Metric(
				"MyBolt7.my.int.elapsed", 59), new Metric(
				"host_1:6701.MyBolt7.my.int.elapsed", 59), new Metric(
				"MyBolt7.my.long", 57), new Metric(
				"host_1:6701.MyBolt7.my.long", 57), new Metric(
				"MyBolt7.my_float", 222), new Metric(
				"host_1:6701.MyBolt7.my_float", 222), new Metric(
				"MyBolt7.my_double", 56), new Metric(
				"host_1:6701.MyBolt7.my_double", 56), new Metric(
				"MyBolt7.points.elapsed", 3321), new Metric(
				"host_1:6701.MyBolt7.points.elapsed", 3321), new Metric(
				"MyBolt7.points.gauge", 1234), new Metric(
				"host_1:6701.MyBolt7.points.gauge", 1234), new Metric(
				"MyBolt7.points.count", 123), new Metric(
				"host_1:6701.MyBolt7.points.count", 123), new Metric(
				"MyBolt7.points.time", 2342234), new Metric(
				"host_1:6701.MyBolt7.points.time", 2342234));

		List<Metric> actual = undertest.dataPointsToMetrics(taskInfo, dataPoints);
		assertEquals(expected, actual);
		for(int i = 0; i < 4; i++) {
			assertEquals(actual.get(i).type, Metric.StatsDType.COUNTER);
		}
		for(int i = 4; i < 6; i++) {
			assertEquals(actual.get(i).type, Metric.StatsDType.GAUGE);
		}
		for(int i = 6; i < 8; i++) {
			assertEquals(actual.get(i).type, Metric.StatsDType.TIMER);
		}
	}
}
