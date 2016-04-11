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

public class ArgumentClass{
    String deploymentMode;  //Local ('L') or Distributed-cluster ('C') Mode
    String topoName;
    String inputDatasetPathName; // Full path along with File Name
    String experiRunId;
    double scalingFactor;  //Deceleration factor with respect to seconds.
    String outputDirName;  //Path where the output log file from spout and sink has to be kept

    public String getOutputDirName() {
        return outputDirName;
    }

    public void setOutputDirName(String outputDirName) {
        this.outputDirName = outputDirName;
    }

    public String getDeploymentMode() {
        return deploymentMode;
    }

    public void setDeploymentMode(String deploymentMode) {
        this.deploymentMode = deploymentMode;
    }

    public String getTopoName() {
        return topoName;
    }

    public void setTopoName(String topoName) {
        this.topoName = topoName;
    }

    public String getInputDatasetPathName() {
        return inputDatasetPathName;
    }

    public void setInputDatasetPathName(String inputDatasetPathName) {
        this.inputDatasetPathName = inputDatasetPathName;
    }

    public String getExperiRunId() {
        return experiRunId;
    }

    public void setExperiRunId(String experiRunId) {
        this.experiRunId = experiRunId;
    }

    public double getScalingFactor() {
        return scalingFactor;
    }

    public void setScalingFactor(double scalingFactor) {
        this.scalingFactor = scalingFactor;
    }
}
