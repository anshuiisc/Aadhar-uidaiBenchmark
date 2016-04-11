#!/bin/bash

# Getting base/parent directory where code is kept
cd ..
baseDirectory=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

# By default 15 mins distribution is given
# Uncommit below two lines  If you want create your own event distribution for 24 hours
#pythonFile=$baseDirectory"/data/auth/scripts/eventGen.py"
#python pythonFile

# Build project
mvn clean compile package

# Jar localtion
stormJarPath=$baseDirectory"/target/uidai-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

# Path of events file
dataPath=$baseDirectory"/data/auth/input/eventDist.csv"

# Path where all output files will be kept
outputPath=$baseDirectory"/data/auth/output"

# Topology name
topologyName="UIDAI_Auth_Topology"

# Experiment name
expNum=$1

# Mode "C" means in Cluster Mode and "L" means in Local Mode
mode="C"

# Run code on Cluster using storm 0.9.4
storm jar $stormJarPath in.dream_lab.bm.uidai.auth.topology.$topologyName $mode $topologyName $dataPath UIDAI-$expNum 1.0 $outputPath

