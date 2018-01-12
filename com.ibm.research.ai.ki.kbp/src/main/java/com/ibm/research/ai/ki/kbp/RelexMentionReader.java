/**
 * cc-dbp-dataset
 *
 * Copyright (c) 2017 IBM
 *
 * The author licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.research.ai.ki.kbp;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

import com.google.common.collect.*;

/**
 * Reads the tsv format for entity-pair (or entity) mentions.
 * @author mrglass
 *
 * @param <T>
 */
public class RelexMentionReader<T extends IRelexMention> implements Iterable<T> {

    public static <T extends IRelexMention> RelexMentionReader<T> getReader(File f, Class<T> clz) {
        return new RelexMentionReader<T>(f, clz);
    }
    
    public static <T extends IRelexMention> RelexMentionReader.SetReader<T> getSetReader(File f, Class<T> clz) {
        return new RelexMentionReader.SetReader<T>(getReader(f, clz));
    }
    
	protected Iterable<String> lines;
	protected Class<T> clz;
	
	public RelexMentionReader(File tsvFile, Class<T> clz) {
	    this.clz = clz;
	    lines = new NestedIterable<File,String>(
	                new FileUtil.FileIterable(tsvFile), 
	                f -> FileUtil.getLines(f.getAbsolutePath()).iterator());
	}
	
	@Override
	public Iterator<T> iterator() {		    
		return Iterators.transform(lines.iterator(), tsvLine -> {
            try {
                T m = clz.newInstance();
                m.fromString(tsvLine);
                return m;
            } catch (Exception e) {
                return Lang.error(e);
            }
		});
	}
	
	/**
	 * Assumes entity-pair groups are continuous
	 * @author mrglass
	 *
	 * @param <T>
	 */
	   public static class SetReader<T extends IRelexMention> implements Iterable<List<T>> {
	        public SetReader(Iterable<T> mentionIterable) {
	            this.mentionIterable = mentionIterable;
	        }
	        protected Iterable<T> mentionIterable;
	        @Override
	        public Iterator<List<T>> iterator() {
	            return new NextOnlyIterator<List<T>>() {
	                Iterator<T> mentionIt = mentionIterable.iterator();
	                T prev = null;
	                @Override
	                protected List<T> getNext() {       
	                    List<T> mentions = new ArrayList<>();
	                    if (prev == null) {
	                        if (!mentionIt.hasNext())
	                            return null;
	                        prev = mentionIt.next();
	                    }
	                    mentions.add(prev);
	                    while (true) {
	                        if (!mentionIt.hasNext()) {
	                            prev = null;
	                            break;
	                        }
	                        prev = mentionIt.next();
	                        if (!mentions.get(0).groupId().equals(prev.groupId()))
	                            break;
	                        mentions.add(prev);
	                    }
	                    return mentions;
	                }           
	            };
	        }       
	    }
}