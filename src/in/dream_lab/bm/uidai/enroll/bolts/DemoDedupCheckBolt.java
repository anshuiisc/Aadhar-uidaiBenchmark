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
public class DemoDedupCheckBolt implements IRichBolt {
    private final String experiRunId;
    OutputCollector _collector;
    int count = 1;

    public DemoDedupCheckBolt(String sinkLogFileName) {
        this.experiRunId=sinkLogFileName;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        BatchedFileLogging.writeToTemp(this, experiRunId);
        LatencyConfig.readFileforOp();
        _collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        LatencyConfig.sleepFortime(LatencyConfig.DEDUP_CHECK_LATENCY);

        String rowString = tuple.getString(0);
        String msgId = tuple.getString(tuple.size()-1);

        if(count < 70) {
            _collector.emit("pass",new Values(rowString, msgId));
            count++;
        }
        else {
            _collector.emit("fail",new Values(rowString, msgId));
            count = 1;
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("pass",new Fields("Column","MSGID"));
        outputFieldsDeclarer.declareStream("fail",new Fields("Column","MSGID"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
