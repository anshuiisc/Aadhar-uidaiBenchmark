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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import in.dream_lab.eventgen.logging.BatchedFileLogging;
import java.lang.management.ManagementFactory;
import java.util.Map;

public class Sink extends BaseRichBolt {

    OutputCollector collector;
    BatchedFileLogging ba;
    String csvFileNameOutSink;

    public Sink(){

    }

    public Sink(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
        //ba=new BatchedFileLogging();
        ba=new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId());

        System.out.println("SinkBolt PID,"+ ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("MSGID");
        //collector.emit(input,new Values(msgId));
        try {
            //        ba.batchLogwriter(System.nanoTime(),msgId);
            ba.batchLogwriter(System.currentTimeMillis(),msgId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //collector.ack(input);
    }


//    @Override
//    public void execute(Tuple input) {
////input.ge
//        String sentence = input.getString(0);
//        collector.emit(input,new Values(sentence ));
//        //System.out.println("===================sink=="+sentence+"$$$$$$$$$");
//        try {
//            ba.batchLogwriter(System.nanoTime(),sentence);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
////        collector.ack(tuple);
//        collector.ack(input);
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
