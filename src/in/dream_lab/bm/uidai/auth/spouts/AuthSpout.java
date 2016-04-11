///**
// * Copyright 2015 DREAM:Lab, Indian Institute of Science, Bangalore
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package in.dream_lab.bm.uidai.auth.spouts;
//
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
//import in.dream_lab.bm.uidai.enroll.config.LatencyConfig;
//import in.dream_lab.eventgen.EventGen;
//import in.dream_lab.eventgen.ISyntheticEventGen;
//import in.dream_lab.eventgen.logging.BatchedFileLogging;
//
//import java.io.IOException;
//import java.lang.management.ManagementFactory;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.nio.charset.StandardCharsets;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class AuthSpout extends BaseRichSpout implements ISyntheticEventGen {
//	SpoutOutputCollector _collector;
//	EventGen eventGen;
//	BlockingQueue<List<String>> eventQueue;
//	String csvFileName;
//	String outSpoutCSVLogFileName;
//	String experiRunId;
//	double scalingFactor;
//	BatchedFileLogging ba;
//	long msgId;
//	String rowString1=null;String rowString2=null;String rowString3=null;String rowString4=null;String rowString5=null;String rowString6=null;
//
//	public AuthSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
//		this.csvFileName = csvFileName;
//		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
//		this.scalingFactor = scalingFactor;
//		this.experiRunId = experiRunId;
//	}
//
//	public AuthSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
//		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
//	}
//
//	@Override
//	public void nextTuple() {
//		// TODO Auto-generated method stub
////		try {
//		System.out.println("spout Queue count= "+this.eventQueue.size());
//		// allow multiple tuples to be emitted per next tuple.
//		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
//		int count = 0, MAX_COUNT=10; // FIXME?
//		while(count < MAX_COUNT) {
//			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
//			if(entry == null) return;
//			count++;
//			Values values = new Values();
//			StringBuilder rowStringBuf = new StringBuilder();
//			for(String s : entry){
//				rowStringBuf.append(",").append(s);
//			}
//			String rowString = rowStringBuf.toString().substring(1);
//
//			System.out.println("rowString--"+rowString+"entry is --"+ entry.get(entry.size()-1));
//			int file_id= Integer.parseInt(entry.get(entry.size() - 1));
//
////			if(file_id==1.0)
//				rowString=rowString4;
////			else if(file_id==2)
////				rowString=rowString2;
////			else if(file_id==3)
////				rowString=rowString3;
////			else if(file_id==4)
////				rowString=rowString4;
////			else if(file_id==5)
////				rowString=rowString5;
////			else if(file_id==6)
////				rowString=rowString6;
//
//			 rowString = rowStringBuf.toString().substring(1);
//			//COMMENT ABOVE LINE For Actual Experiment
//			values.add(rowString);
//			msgId++;
//			values.add(Long.toString(msgId));
//			this._collector.emit(values);
//			try {
////				msgId++;
//
//				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
//				//ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
////			System.out.println("values by source are -" + values);
////		} catch (InterruptedException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//		}
//	}
//
//	@Override
//	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
//		try {
////			rowString1= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_1MB", StandardCharsets.UTF_8);
////			rowString2= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_2MB", StandardCharsets.UTF_8);
////			rowString3= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_3MB", StandardCharsets.UTF_8);
//			rowString4= LatencyConfig.readFileWithSize("/Users/anshushukla/Downloads/Incomplete/stream/benchmark/src/in/dream_lab/bm/uidai/auth/raw_files/file_4KB", StandardCharsets.UTF_8);
////			rowString1= LatencyConfig.readFileWithSize("/data/storm/dataset/file_4KB", StandardCharsets.UTF_8);
////			rowString4= LatencyConfig.readFileWithSize("/data/storm/dataset/file_4KB", StandardCharsets.UTF_8);
////			rowString5= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_5MB", StandardCharsets.UTF_8);
////			rowString6= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_6MB", StandardCharsets.UTF_8);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		System.out.println("AuthSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
//		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
//		Random r=new Random();
//		try {
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-01.local")==0)
//				msgId= (long) (1*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-02.local")==0)
//				msgId= (long) (2*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-03.local")==0)
//				msgId= (long) (3*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-04.local")==0)
//				msgId= (long) (4*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-05.local")==0)
//				msgId= (long) (5*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-06.local")==0)
//				msgId= (long) (6*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-07.local")==0)
//				msgId= (long) (7*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-08.local")==0)
//				msgId= (long) (8*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-09.local")==0)
//				msgId= (long) (9*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("orion-10.local")==0)
//				msgId= (long) (10*Math.pow(10,8)+r.nextInt(100));
//			if(InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local")==0)
//				msgId= (long) (10*Math.pow(10,8)+r.nextInt(100));
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
////		msgId= Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]+Integer.toString(r.nextInt(100)));
//
//		_collector = collector;
//		this.eventGen = new EventGen(this,this.scalingFactor);
//		this.eventQueue = new LinkedBlockingQueue<List<String>>();
//		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
//		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads
//
//		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());
//
//
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("RowString", "MSGID"));
//	}
//
//	@Override
//	public void receive(List<String> event) {
//		try {
//			this.eventQueue.put(event);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//}


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

package in.dream_lab.bm.uidai.auth.spouts;

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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AuthSpout extends BaseRichSpout implements ISyntheticEventGen {
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

	public AuthSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public AuthSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
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

			//System.out.println("rowString--"+rowString+"entry is --"+ entry.get(entry.size()-1));
			//int file_id= Integer.parseInt(entry.get(entry.size() - 1));
			int file_id=1;
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
	//		System.out.println("CHECK:" + rowString);

			//	rowString = rowStringBuf.toString().substring(1);
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
			rowString1= LatencyConfig.readFileWithSize("/data/storm/dataset/file_4KB", StandardCharsets.UTF_8);
		//	rowString1= LatencyConfig.readFileWithSize("/Users/anshushukla/Downloads/Incomplete/stream/benchmark/src/in/dream_lab/bm/uidai/auth/raw_files/file_4KB", StandardCharsets.UTF_8);
			rowString2= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_2MB", StandardCharsets.UTF_8);
			rowString3= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_3MB", StandardCharsets.UTF_8);
			rowString4= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_4MB", StandardCharsets.UTF_8);
			rowString5= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_5MB", StandardCharsets.UTF_8);
			rowString6= LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_6MB", StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("AuthSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);

		Random r=new Random();
		try {
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-01.local")==0)
				msgId= (long) (1*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-02.local")==0)
				msgId= (long) (2*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-03.local")==0)
				msgId= (long) (3*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-04.local")==0)
				msgId= (long) (4*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-05.local")==0)
				msgId= (long) (5*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-06.local")==0)
				msgId= (long) (6*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-07.local")==0)
				msgId= (long) (7*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-08.local")==0)
				msgId= (long) (8*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-09.local")==0)
				msgId= (long) (9*Math.pow(10,8)+r.nextInt(100));
			if(InetAddress.getLocalHost().getHostName().compareTo("orion-10.local")==0)
				msgId= (long) (10*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-11.local")==0)
                                msgId= (long) (11*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-12.local")==0)
                                msgId= (long) (12*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-13.local")==0)
                                msgId= (long) (13*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-14.local")==0)
                                msgId= (long) (14*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-15.local")==0)
                                msgId= (long) (15*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-16.local")==0)
                                msgId= (long) (16*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-17.local")==0)
                                msgId= (long) (17*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-18.local")==0)
                                msgId= (long) (18*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-19.local")==0)
                                msgId= (long) (19*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-20.local")==0)
                                msgId= (long) (20*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-21.local")==0)
                                msgId= (long) (21*Math.pow(10,8)+r.nextInt(100));
			 if(InetAddress.getLocalHost().getHostName().compareTo("orion-22.local")==0)
                                msgId= (long) (22*Math.pow(10,8)+r.nextInt(100));
                         if(InetAddress.getLocalHost().getHostName().compareTo("orion-23.local")==0)
                                msgId= (long) (23*Math.pow(10,8)+r.nextInt(100));
                         if(InetAddress.getLocalHost().getHostName().compareTo("orion-24.local")==0)
                                msgId= (long) (24*Math.pow(10,8)+r.nextInt(100));		
			if(InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local")==0)
				msgId= (long) (10*Math.pow(10,8)+r.nextInt(100));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
//		msgId=r.nextInt(10000);
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
