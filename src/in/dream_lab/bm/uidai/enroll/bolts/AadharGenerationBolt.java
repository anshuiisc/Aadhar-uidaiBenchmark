/**
 * Copyright 2015 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package in.dream_lab.bm.uidai.enroll.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.bm.uidai.enroll.config.LatencyConfig;
import in.dream_lab.eventgen.logging.BatchedFileLogging;

import java.util.Map;

public class AadharGenerationBolt implements IRichBolt {
    String experiRunId;
    OutputCollector _collector;

    public AadharGenerationBolt(String sinkLogFileName) {
        this.experiRunId = sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);

        LatencyConfig.readFileforOp();
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        LatencyConfig.sleepFortime(LatencyConfig.POST_AADHAR_LATENCY);

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size() - 1);

        _collector.emit(new Values(rowString, msgId));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Column", "MSGID"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
