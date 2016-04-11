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

package in.dream_lab.eventgen;

import in.dream_lab.eventgen.factory.CsvSplitter;
import in.dream_lab.eventgen.factory.TableClass;
import in.dream_lab.eventgen.utils.GlobalConstants;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class EventGen {
    ISyntheticEventGen iseg;
    ExecutorService executorService;
    double scalingFactor;

    public EventGen(ISyntheticEventGen iseg) {
        this(iseg, GlobalConstants.accFactor);
    }

    public EventGen(ISyntheticEventGen iseg, double scalingFactor) {
        this.iseg = iseg;
        this.scalingFactor = scalingFactor;
    }

    public static List<String> getHeadersFromCSV(String csvFileName) {
        return CsvSplitter.extractHeadersFromCSV(csvFileName);
    }

    public void launch(String csvFileName, String outCSVFileName) {
        launch(csvFileName, outCSVFileName, -1);
    }

    //Launches all the threads
    public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis) {
        //1. Load CSV to in-memory data structure
        //2. Assign a thread with (new SubEventGen(myISEG, eventList))
        //3. Attach this thread to ThreadPool
        System.out.println("scalingfactor"+scalingFactor+"-"+csvFileName+"-"+experimentDurationMillis);
        try {
            int numThreads = GlobalConstants.numThreads;
            //double scalingFactor = GlobalConstants.accFactor;
            String datasetType = "";
            if (outCSVFileName.indexOf("TAXI") != -1) {
                datasetType = "TAXI";// GlobalConstants.dataSetType = "TAXI";
            } else if (outCSVFileName.indexOf("SYS") != -1) {
                datasetType = "SYS";// GlobalConstants.dataSetType = "SYS";
            } else if (outCSVFileName.indexOf("PLUG") != -1) {
                datasetType = "PLUG";// GlobalConstants.dataSetType = "PLUG";
            }  else if (outCSVFileName.indexOf("UIDAI") != -1) {
                datasetType = "UIDAI";// GlobalConstants.dataSetType = "UIDAI";
            }

            List<TableClass> nestedList = CsvSplitter.roundRobinSplitCsvToMemory(csvFileName, numThreads, scalingFactor, datasetType);

            this.executorService = Executors.newFixedThreadPool(numThreads);

            Semaphore sem1 = new Semaphore(0);

            Semaphore sem2 = new Semaphore(0);

            SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2);
                this.executorService.execute(subEventGenArr[i]);
            }

            sem1.acquire(numThreads);
            //set the start time to all the thread objects
            long experiStartTs = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i].experiStartTime = experiStartTs;
                if (experimentDurationMillis > 0) subEventGenArr[i].experiDuration = experimentDurationMillis;
                this.executorService.execute(subEventGenArr[i]);
            }
            sem2.release(numThreads);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

class SubEventGen implements Runnable {
    ISyntheticEventGen iseg;
    TableClass eventList;
    Long experiStartTime;  //in millis since epoch
    Semaphore sem1, sem2;
    Long experiDuration = -1L;

    public SubEventGen(ISyntheticEventGen iseg, TableClass eventList, Semaphore sem1, Semaphore sem2) {
        this.iseg = iseg;
        this.eventList = eventList;
        this.sem1 = sem1;
        this.sem2 = sem2;
    }

    @Override
    public void run() {

        sem1.release();
        try {
            sem2.acquire();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.println("THREAD: " + Thread.currentThread().getName() + " size of eventList = " + this.eventList.getRows().size() + " relativeTs= " + this.eventList.getTs().get(0) + " TRY to call receive ###" + " this.experiStartTs = " + this.experiStartTime);

        List<List<String>> rows = this.eventList.getRows();
        int rowLen = rows.size();
        List<Long> timestamps = this.eventList.getTs();
        Long experiRestartTime = experiStartTime;
        boolean runOnce = (experiDuration < 0);
        long currentRuntime = 0;

        do {
            for (int i = 0; i < rowLen && (runOnce || (currentRuntime < experiDuration)); i++) {
                Long deltaTs = timestamps.get(i);
                List<String> event = rows.get(i);
                Long currentTs = System.currentTimeMillis();
                long delay = deltaTs - (currentTs - experiRestartTime); // how long until this event should be sent?
                if (delay > 10) { // sleep only if it is non-trivial time. We will catch up on sleep later.
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                this.iseg.receive(event);

                currentRuntime = (currentTs - experiStartTime) + delay; // appox time since the experiment started
            }

            experiRestartTime = System.currentTimeMillis();
        } while (!runOnce && (currentRuntime < experiDuration));

    }
}
