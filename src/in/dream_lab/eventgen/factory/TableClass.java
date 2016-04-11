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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//NOTE: NOT Thread-safe
public class TableClass implements Iterable<RowClass>, Iterator<RowClass> {
	List<Long> ts;
	List<String> header;
	List<List<String>> rows;
	int index;

	public TableClass(){
		this.ts = new ArrayList<Long>();
		this.header = new ArrayList<String>();
		this.rows = new ArrayList<List<String>>();
	}

	public void append(Long ts, List<String> row){
		this.ts.add(ts);
		this.rows.add(row);
	}

	public List<Long> getTs() {
		return ts;
	}

	public void setTs(List<Long> ts) {
		this.ts = ts;
	}

	public List<String> getHeader() {
		return header;
	}

	public void setHeader(List<String> header) {
		this.header = header;
	}

	public List<List<String>> getRows() {
		return rows;
	}

	public void setRows(List<List<String>> rows) {
		this.rows = rows;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return index < rows.size();
//		if(index == rows.size())
//			return false;
//		return true;
	}

	@Override
	public RowClass next() {
		// TODO Auto-generated method stub
		index++;
		return new RowClass(this.ts.get(index-1), this.header, this.rows.get(index-1));
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<RowClass> iterator() {
		// TODO Auto-generated method stub
		this.index = 0;
		return this;
	}
}
