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

package in.dream_lab.bm.uidai.enroll.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import in.dream_lab.bm.uidai.enroll.config.LatencyConfig;
import in.dream_lab.eventgen.EventGen;
import in.dream_lab.eventgen.ISyntheticEventGen;
import in.dream_lab.eventgen.logging.BatchedFileLogging;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AadhaarSpout extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;
	String rowString1=null;String rowString2=null;String rowString3=null;String rowString4=null;String rowString5=null;String rowString6=null;

	public AadhaarSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public AadhaarSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		try {
		System.out.println("spout Queue count= "+this.eventQueue.size());
		// allow multiple tuples to be emitted per next tuple.
		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
		int count = 0, MAX_COUNT=10; // FIXME?
		while(count < MAX_COUNT) {
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			Values values = new Values();
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);

			System.out.println("rowString--"+rowString+"entry is --"+ entry.get(entry.size()-1));
			int file_id= Integer.parseInt(entry.get(entry.size() - 1));

			if(file_id==1)
				rowString=rowString1;
			else if(file_id==2)
				rowString=rowString2;
			else if(file_id==3)
				rowString=rowString3;
			else if(file_id==4)
				rowString=rowString4;
			else if(file_id==5)
				rowString=rowString5;
			else if(file_id==6)
				rowString=rowString6;

			// rowString = rowStringBuf.toString().substring(1);
			//COMMENT ABOVE LINE For Actual Experiment
			values.add(rowString);
			msgId++;
			values.add(Long.toString(msgId));
			this._collector.emit(values);
			try {
//				msgId++;
				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
				//ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
//			System.out.println("values by source are -" + values);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		try {
			rowString1= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_1MB", StandardCharsets.UTF_8);
			rowString2= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_2MB", StandardCharsets.UTF_8);
			rowString3= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_3MB", StandardCharsets.UTF_8);
			rowString4= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_4MB", StandardCharsets.UTF_8);
			rowString5= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_5MB", StandardCharsets.UTF_8);
			rowString6= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_6MB", StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("AadhaarSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();
		long msgId=r.nextInt(10000);
		_collector = collector;
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("RowString", "MSGID"));
	}

	@Override
	public void receive(List<String> event) {
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
