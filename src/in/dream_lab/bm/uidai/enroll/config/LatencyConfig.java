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

package in.dream_lab.bm.uidai.enroll.config;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LatencyConfig {

   // Latency for PacketExtractionBolt
   public static final int PACKET_EXTRACTION_LATENCY = 2770;

   // Latency for DedupCheckBolt
   public static final int DEDUP_CHECK_LATENCY = 2400;

   // Latency for QualityCheckBolt
   public static final int QUALITY_CHECK_LATENCY = 10000;

   // Latency for PacketValidationBolt
   public static final int PACKET_VALIDATION_LATENCY = 2400;

   // Latency for BioDedupBolt
   public static final int BIO_DEDUP_LATENCY = 2850;

   // Latency for ManualDedupCheckBolt
   public static final int MANUAL_DEDUP_LATENCY = 4800;

   // Latency for AadharGenerationBolt
   public static final int POST_AADHAR_LATENCY = 800;

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
