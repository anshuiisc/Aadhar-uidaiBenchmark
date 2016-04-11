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

package in.dream_lab.bm.uidai.auth.config;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LatencyConfig {


//    // Latency for HTTPRequestHandlingBolt
//    public static final int HTTP_REQUEST_HANDLING_LATENCY = (int) 443.3/2;
//
//    // Latency for DedupCheckBolt
//    public static final int VERIFICATION_LATENCY = (int) (5+5+22+12.1)/2;
//
//    // Latency for QualityCheckBolt
//    public static final int VALIDATION_LATENCY = (int) (5+26.4+5+13.2+5+5+5)/2;
//
//    // Latency for PacketValidationBolt
//    public static final int READ_RESIDENT_DATA_LATENCY = (int) (27.5)/2;
//
//    // Latency for BioDedupBolt
//    public static final int MATCHING_LATENCY = (int) (5.0+5+5)/2;
//
//    // Latency for ManualDedupCheckBolt
//    public static final int BIODEDUP_MATCHING_LATENCY = (int) (50.6)/2;
//
//    // Latency for AadharGenerationBolt
//    public static final int CREATE_RESPONSE_LATENCY = (int) (5+12.1+12.1)/2;
//
//    // Latency for ManualDedupCheckBolt
//    public static final int LOGGING_METRICS_LATENCY = (int) (173.8+5+786.5)/2;
//
//    // Latency for AadharGenerationBolt
//    public static final int SEND_RESPONSE_LATENCY = (int) (443.3)/2;




    //old latency [90,4,1,,5,1,10,5,31,90]
    //new Latency [26, 20, 13, 19, 38, 2, 18, 114]
    public static final int VALIDATION_LATENCY = 26;
    public static final int DECRYPTION_LATENCY = 20;
    public static final int VERIFICATION_LATENCY = 13;
    public static final int READ_RESIDENT_DATA_LATENCY = 19;
    public static final int MATCHING_LATENCY = 38;
    public static final int NOTIFICATION_LATENCY = 2;
    public static final int CREATE_RESPONSE_LATENCY = 18;
    public static final int SEND_RESPONSE_LATENCY = 114;

// oldparallelism       [45.0, 2.0, 6.0, 3.0, 1.0,  5.0, 3.0, 16.0, 45.0]
// new parall-          [13.0, 10.0,7.0, 10.0,19.0, 1.0, 9.0, 58.0]
    public static final int VALIDATION_Parallelism =50;//  20;
    public static final int DECRYPTION_Parallelism =38;// 15;
    public static final int VERIFICATION_Parallelism = 30;//11;
    public static final int READ_RESIDENT_DATA_Parallelism = 37;//15;
    public static final int MATCHING_Parallelism =71;// 29;
    public static final int  NOTIFICATION_Parallelism =30 ;//10;
    public static final int CREATE_RESPONSE_Parallelism = 37;//15;
    public static final int SEND_RESPONSE_Parallelism = 210;//87;

    public static String readFileWithSize(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }


    static String rowStringforop = null;

    public static String readFileforOp() {
        if (rowStringforop == null) {
            try {
//                rowStringforop = LatencyConfig.readFileWithSize("src/in/dream_lab/bm/uidai/enroll/raw_files/file_1MB", StandardCharsets.UTF_8);
                System.out.println("Chances of error see comment-");
                rowStringforop = LatencyConfig.readFileWithSize("/data/storm/dataset/file_1MB", StandardCharsets.UTF_8);
            } catch (IOException e) {
                System.out.println("files not readable");
                e.printStackTrace();
            }
        }

        return rowStringforop;

    }

    public static String sleepFortime(long milliSec) {
        String upper = null;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= milliSec) {
            upper = rowStringforop.toUpperCase();
        }
        return upper;
    }


}
