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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowClass {
	long ts;  //DateTime
	Map<String, String> payLoad;

	public RowClass(long ts, Map<String, String> payLoad){
		this.ts = ts;
		this.payLoad = payLoad;
	}

	public RowClass(long ts, List<String> header, List<String> row){
		this.ts = ts;
		this.payLoad = new HashMap<String, String>();
		for(int i=0; i<header.size(); i++){
			this.payLoad.put(header.get(i), row.get(i));
		}
	}

	public RowClass(){
		ts = 0;
		payLoad = null;
	}

	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public Map<String, String> getPayLoad() {
		return payLoad;
	}
	public void setPayLoad(Map<String, String> payLoad) {
		this.payLoad = payLoad;
	}

	public String toString(){
		String out = ts + "," + payLoad;
		return out;
	}
}
