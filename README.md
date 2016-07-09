# Benchmarking Fast Data Systems for the Aadhaar Biometric Database
**Machine Configuration for  Benchmarking**
---
- Apache Storm v0.9.4
- CentOS 7
- OpenJDK v1.7
- 1 node of commodity cluster for running Zookeeper, Nimbus and UI
- 9 nodes of commodity cluster for executing the topologies(supervisors)

**About Code**
---
**Storm Topology** <br/>
- [bolts](https://github.com/dream-lab/bigdata-benchmarks/tree/master/uidai/src/in/dream_lab/stream/bm/uidai/enroll/bolts) directory contains different bolts  used in aadhaar enrollment topology <br/>
- [spout](https://github.com/dream-lab/bigdata-benchmarks/tree/master/uidai/src/in/dream_lab/stream/bm/uidai/enroll/spouts) directory has AadhaarSpout which feed packets to topology<br/>
- [config](https://github.com/dream-lab/bigdata-benchmarks/tree/master/uidai/src/in/dream_lab/stream/bm/uidai/enroll/config) directory contains global configuration files used by topology<br/>
- [topology](https://github.com/dream-lab/bigdata-benchmarks/tree/master/uidai/src/in/dream_lab/stream/bm/uidai/enroll/topology) directory has Enrollment topology <br/>

**Event generation and logging framework**
- Used to create events from given input file according to timestamps values and logs the data generated during execution of topology for future analysis.

**How to Run code**
---
- Go to bin directory and run following command:<br>
 "./runUidaiTopology.sh experimentID" where experimentID is a number of experiment e.g. 100, 101 etc.
- You can change different parameters used in runUidaiTopology.sh file.
- By default 15 minutes events are given, if you want to create 24 hours events then either make changes in runUidaiTopology.sh file or run "python /path/to/.../data/enroll/scripts/eventGen.py" on terminal.


<!--<center> ** </center>-->

Please refer the paper for detailed info  - <http://arxiv.org/pdf/1510.04160v2.pdf> 




