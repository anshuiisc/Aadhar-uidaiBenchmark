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

package in.dream_lab.bm.uidai.auth.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.bm.uidai.auth.bolts.*;
import in.dream_lab.bm.uidai.auth.config.LatencyConfig;
import in.dream_lab.bm.uidai.auth.spouts.AuthSpout;
import in.dream_lab.eventgen.factory.ArgumentClass;
import in.dream_lab.eventgen.factory.ArgumentParser;

public class UIDAI_Auth_Topology {
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
        config.setDebug(false);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("auth-spout", new AuthSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 10);


//        builder.setBolt("http-request-handling-bolt", new ValidationBolt(sinkLogFileName),
//                LatencyConfig.VALIDATION_Parallelism)
//                .shuffleGrouping("auth-spout");

        builder.setBolt("validation-bolt", new ValidationBolt(sinkLogFileName),
                LatencyConfig.VALIDATION_Parallelism)
                .shuffleGrouping("auth-spout");

        builder.setBolt("decryption-bolt", new DecryptBolt(sinkLogFileName),
                LatencyConfig.DECRYPTION_Parallelism)
                .shuffleGrouping("validation-bolt", "pass");

        builder.setBolt("verification-bolt", new VerificationBolt(sinkLogFileName),
                LatencyConfig.VERIFICATION_Parallelism)
                .shuffleGrouping("decryption-bolt", "pass");

        builder.setBolt("read-resident-data-bolt", new ReadResidentData(sinkLogFileName),
                LatencyConfig.READ_RESIDENT_DATA_Parallelism)
                .shuffleGrouping("verification-bolt", "pass");

        builder.setBolt("matching-bolt", new MatchingBolt(sinkLogFileName),
                LatencyConfig.MATCHING_Parallelism)
                .shuffleGrouping("read-resident-data-bolt", "pass");

        builder.setBolt("notification-bolt", new NotificationBolt(sinkLogFileName),
                LatencyConfig.NOTIFICATION_Parallelism)
                .shuffleGrouping("matching-bolt", "pass");

        builder.setBolt("create-response-bolt", new CreateResponseBolt(sinkLogFileName),
                LatencyConfig.CREATE_RESPONSE_Parallelism)
                .shuffleGrouping("notification-bolt", "pass");


        builder.setBolt("send-response-bolt", new SendResponseBolt(sinkLogFileName),
                LatencyConfig.SEND_RESPONSE_Parallelism)
                .shuffleGrouping("create-response-bolt", "pass");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1)
                .shuffleGrouping("send-response-bolt", "pass");


//        builder.setBolt("fail-sink", new Sink(sinkLogFileNameFail), 1)
//                .shuffleGrouping("http-request-handling-bolt", "fail")
//                .shuffleGrouping("validation-bolt","fail")
//                .shuffleGrouping("verification-bolt", "fail")
//                .shuffleGrouping("read-resident-data-bolt","fail")
//                .shuffleGrouping("matching-bolt", "fail")
//                .shuffleGrouping("bio-dedup-matching-bolt", "fail")
//                .shuffleGrouping("create-response-bolt","fail")
//                .shuffleGrouping("logging-metrics-bolt", "fail")
//                .shuffleGrouping("send-response-bolt", "fail");


        StormTopology stormTopology = builder.createTopology();
        if (argumentClass.getDeploymentMode().equals("C")) {
config.setNumWorkers(192);            
//config.setNumWorkers((stormTopology.get_spouts_size() + stormTopology.get_bolts_size()));
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
* L UIDAI_Auth_Topology /home/dell/uidaiTopology/uidaiCode/uidai/data/auth/input/eventDist.csv UIDAI-100  0.01  /home/dell/uidaiTopology/uidaiCode/uidai/data/auth/output
* L UIDAI_Auth_Topology /path/to/.../data/auth/input/eventDist.csv UIDAI-100  0.01  /path/to/.../data/auth/output
*
* L UIDAI_Auth_Topology /Users/anshushukla/Downloads/Incomplete/stream/benchmark/data/auth/input/scaledUidaiInput864.csv  UIDAI-107  0.01  /Users/anshushukla/Downloads/Incomplete/stream/benchmark/data/auth/output
*/
