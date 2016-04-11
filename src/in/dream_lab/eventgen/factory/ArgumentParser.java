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

package in.dream_lab.eventgen.factory;

import java.net.InetAddress;

public class ArgumentParser {

    /*
    Convention is:
    Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor>
    Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001
     */
    public static ArgumentClass parserCLI(String [] args){
        if(args == null || args.length != 6){
            System.out.println("invalid number of arguments");
            return null;
        }
        else {
            ArgumentClass argumentClass = new ArgumentClass();
            argumentClass.setDeploymentMode(args[0]);
            argumentClass.setTopoName(args[1]);
            argumentClass.setInputDatasetPathName(args[2]);
            argumentClass.setExperiRunId(args[3]);
            argumentClass.setScalingFactor(Double.parseDouble(args[4]));
            argumentClass.setOutputDirName(args[5]);
            return argumentClass;
        }
    }

    public static void main(String [] args){
        try {
            System.out.println(InetAddress.getLocalHost().getHostName());
        }catch(Exception e){
            e.printStackTrace();
        }
        ArgumentClass argumentClass = parserCLI(args);
        if(argumentClass == null){
            System.out.println("Improper Arguments");
        }
        else{
            System.out.println(argumentClass.getDeploymentMode() +" : " + argumentClass.getExperiRunId() + ":" + argumentClass.getScalingFactor());
        }
    }
}
