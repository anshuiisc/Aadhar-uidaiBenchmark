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

package in.dream_lab.bm.uidai.enroll.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.bm.uidai.enroll.bolts.*;
import in.dream_lab.bm.uidai.enroll.spouts.AadhaarSpout;
import in.dream_lab.eventgen.factory.ArgumentClass;
import in.dream_lab.eventgen.factory.ArgumentParser;

public class UIDAI_Enroll_Topology {
    public static void main(String[] args) throws Exception{
        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
        }
        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + "-" + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String sinkLogFileNameFail = argumentClass.getOutputDirName() + "/sink-Fail" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        /** Common Code ends **/

        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("read-packet-spout", new AadhaarSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1).setNumTasks(1);

        builder.setBolt("packet-extraction-bolt", new PacketExtractionBolt(sinkLogFileName), 50)
                .shuffleGrouping("read-packet-spout");

        builder.setBolt("demographic-check-bolt", new DemoDedupCheckBolt(sinkLogFileName),44)
                .shuffleGrouping("packet-extraction-bolt");

        builder.setBolt("quality-dedup-check-bolt", new QualityCheckBolt(sinkLogFileName),180)
                .shuffleGrouping("demographic-check-bolt","pass");

        builder.setBolt("packet-validation-bolt", new PacketValidationBolt(sinkLogFileName),44)
                .shuffleGrouping("quality-dedup-check-bolt","pass");

        builder.setBolt("biometric-dedup-bolt", new BioDedupBolt(sinkLogFileName),52)
                .shuffleGrouping("packet-validation-bolt","pass");

        builder.setBolt("aadhar-gen-bolt", new AadharGenerationBolt(sinkLogFileName),15)
                .shuffleGrouping("biometric-dedup-bolt","pass");

        builder.setBolt("manual-dedup", new ManualDedupCheckBolt(sinkLogFileName),87)
                .shuffleGrouping("biometric-dedup-bolt","fail");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1)
                .shuffleGrouping("aadhar-gen-bolt");

        builder.setBolt("fail-sink", new Sink(sinkLogFileNameFail), 1)
                .shuffleGrouping("demographic-check-bolt", "fail")
                .shuffleGrouping("quality-dedup-check-bolt","fail")
                .shuffleGrouping("packet-validation-bolt", "fail")
                .shuffleGrouping("manual-dedup");

        StormTopology stormTopology = builder.createTopology();
        if (argumentClass.getDeploymentMode().equals("C")) {
            config.setNumWorkers((stormTopology.get_spouts_size() + stormTopology.get_bolts_size()));
            StormSubmitter.submitTopology(argumentClass.getTopoName(), config, stormTopology);
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), config, stormTopology);
            Utils.sleep(1000000000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }

    }
}

/* Give below arguments if you are going to run topology from IDE
* L UIDAI_Enroll_Topology /home/dell/uidaiTopology/uidaiCode/uidai/data/enroll/input/eventDist.csv UIDAI-100  0.01  /home/dell/uidaiTopology/uidaiCode/uidai/data/enroll/output
* L UIDAI_Enroll_Topology /path/to/.../data/enroll/input/eventDist.csv UIDAI-100  0.01  /path/to/.../data/enroll/output
*
*
* L UIDAI_Enroll_Topology /Users/anshushukla/Downloads/Incomplete/stream/benchmark/data/enroll/input/eventDist.csv  UIDAI-107  0.01  /Users/anshushukla/Downloads/Incomplete/stream/benchmark/data/enroll/output
*/