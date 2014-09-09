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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * @author Jason Trost, jsongHBO
 */
public class StatsdMetricConsumer implements IMetricsConsumer {

    public static final Logger LOG = LoggerFactory.getLogger(StatsdMetricConsumer.class);

    public static final String STATSD_HOST = "metrics.statsd.host";
    public static final String STATSD_PORT = "metrics.statsd.port";
    public static final String STATSD_PREFIX = "metrics.statsd.prefix";

    String topologyName;
    String statsdHost;
    int statsdPort = 8125;
    String statsdPrefix = "storm.metrics.";

    transient StatsDClient statsd;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {
        parseConfig(stormConf);

        if (registrationArgument instanceof Map) {
            parseConfig((Map) registrationArgument);
        }

        statsd = new NonBlockingStatsDClient(statsdPrefix + clean(topologyName), statsdHost, statsdPort);
    }

    void parseConfig(@SuppressWarnings("rawtypes") Map conf) {
        if (conf.containsKey(Config.TOPOLOGY_NAME)) {
            topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        }

        if (conf.containsKey(STATSD_HOST)) {
            statsdHost = (String) conf.get(STATSD_HOST);
        }

        if (conf.containsKey(STATSD_PORT)) {
            if (conf.get(STATSD_PORT) instanceof String) {
                statsdPort = Integer.parseInt((String) conf.get(STATSD_PORT));
            } else {
                statsdPort = ((Number) conf.get(STATSD_PORT)).intValue();
            }
        }

        if (conf.containsKey(STATSD_PREFIX)) {
            statsdPrefix = (String) conf.get(STATSD_PREFIX);
            if (!statsdPrefix.endsWith(".")) {
                statsdPrefix += ".";
            }
        }
    }

    String toCamelCase(String s) {
        StringBuilder sb = new StringBuilder();
        for(String part: s.split("_") ) {
            if (part.length() > 0) {
                sb.append(Character.toUpperCase(part.charAt(0)));
            }
            if (part.length() > 1) {
                sb.append(part.substring(1).toLowerCase());
            }
        }
        return sb.toString();
    }

    String clean(String s) {
        return s.replace('/', '_').toLowerCase();
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {
        for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
            report(metric);
        }
    }

    public static class Metric {
        public enum StatsDType {
            TIMER, COUNTER, GAUGE
        }

        String name;
        Number value;
        StatsDType type;

        public Metric(String name, Number value) {
            this.name = name;
            this.value = value;
            this.type = getTypeFromName(name);
        }

        // Storm doesn't provide any way to convert their Metrics to StatsD's equivalent.
        // So we have to base on the name of the metric
        public StatsDType getTypeFromName(String name) {
            // making sure they're the last characters
            // elapsed is always a timer
            if (name.lastIndexOf(".elapsed") == (name.length() - 8)) {
                return StatsDType.TIMER;
            }
            if (name.lastIndexOf(".gauge") == (name.length() - 6)) {
                return StatsDType.GAUGE;
            }
            return StatsDType.COUNTER;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Metric other = (Metric) obj;
            if (name == null) {
                if (other.name != null) {
                    return false;
                }
            } else if (!name.equals(other.name)) {
                return false;
            }
            if (value.longValue() != other.value.longValue()) {
                return false;
            }
            if (type != other.type) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Metric [name=" + name + ", value=" + value + "]";
        }
    }

    List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {
        List<Metric> res = new LinkedList<>();

        // we don't want to keep things like "__system"
        if (rejectMetric(clean(taskInfo.srcComponentId))) {
            return res;
        }

        // setup the header for the metrics pertained to this machine
        StringBuilder sb = new StringBuilder()
                .append(clean(taskInfo.srcWorkerHost).replace(".", "_")).append(":")
                .append(taskInfo.srcWorkerPort).append(".");

        int machineHrdLength = sb.length();

        sb.append(toCamelCase(clean(taskInfo.srcComponentId))).append(".");

        int hdrLength = sb.length();

        for (DataPoint p : dataPoints) {

            String name = clean(p.name);
            // we don't want to keep things like "__acker"
            if (rejectMetric(name)) {
                continue;
            }

            sb.delete(hdrLength, sb.length());
            sb.append(name);

            if (p.value instanceof Number) {
                res.add(new Metric(sb.substring(machineHrdLength), ((Number) p.value).longValue()));
                res.add(new Metric(sb.toString(), ((Number) p.value).longValue()));
            } else if (p.value instanceof Map) {
                int hdrAndNameLength = sb.length();
                @SuppressWarnings("rawtypes")
                Map map = (Map) p.value;
                for (Object subName : map.keySet()) {
                    String subNameStr = clean(subName.toString());
                    // we don't want to keep things like "__receive" or "__sendqueue"
                    if (rejectMetric(subNameStr)) {
                        continue;
                    }
                    Object subValue = map.get(subName);
                    if (subValue instanceof Number) {
                        sb.delete(hdrAndNameLength, sb.length());
                        sb.append(".").append(subNameStr);

                        res.add(new Metric(sb.substring(machineHrdLength), ((Number) subValue).longValue()));
                        res.add(new Metric(sb.toString(), ((Number) subValue).longValue()));
                    }
                }
            }
        }
        return res;
    }

    // All Storm internal metrics will start with '__' (after a clean()).  We don't yet find any of those information useful.
    // So we're ignoring default Storm metrics and only care about our own.
    private boolean rejectMetric(String name) {
        return (name.indexOf("__") == 0);
    }

    public void report(Metric metric) {
        String s = metric.name;
        switch(metric.type) {
            case TIMER: {
                long number = metric.value.longValue();
                LOG.debug("reporting: {}={}", s, number);
                statsd.time(s, number);
                break;
            }
            case GAUGE: {
                long number = metric.value.longValue();
                LOG.debug("reporting: {}={}", s, number);
                statsd.gauge(s, number);
                break;
            }
            default: {
                long number = metric.value.longValue();
                LOG.debug("reporting: {}={}", s, number);
                statsd.count(s, number);
            }
        }
    }

    @Override
    public void cleanup() {
        statsd.stop();
    }
}
