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
import java.nio.file.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

/**
 * For single-machine. Used at the end of CreateTsvDataset, to group the mentions by id-pair.
 * @author mrglass
 *
 */
public class GroupRelexMentionTsvDataset {
	static final double maxFileSize = 1000000000;
	
	
	
	private static void groupFile(IRelexMention m, RelexConfig config, String f) {
		//build a map idPair->mentionSet and overwrite it
		Map<String,ArrayList<String>> id2line = new HashMap<>();
		for (String line : FileUtil.getLines(f)) {
		    m.fromString(line);
			HashMapUtil.addAL(id2line, m.groupId(), line);
		}
		PrintStream out = FileUtil.getFilePrintStream(f);
		Set<String> dedup = new HashSet<>();
		for (List<String> lines : id2line.values()) {
		    dedup.clear();
			for (String line : lines) {
			    if (dedup.size() < config.maxMentionSet * config.maxMentionGroups && 
			        dedup.add(line.substring(0, line.lastIndexOf('\t')))) 
			    {
			        out.println(line);
			    }
			}
		}
		out.close();
	}

	//CONSIDER: limit number of open files - some systems may have low 'ulimit -n'
	public static void splitAndSort(RelexConfig config, File tsvFile) {
	    IRelexMention m = null;
	    try {
    	    IRelexDatasetManager rdm = config.getManager();
            Class<IRelexMention> mc = rdm.getMentionClass();
    	    m = mc.newInstance();
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw new Error("Could not construct mention class from relex config: "+e.getMessage());
	    }
	    
		long len = tsvFile.length();
		int numParts = (int)Math.ceil((double)len/maxFileSize);
		if (numParts == 1) {
		    String onePart = FileUtil.removeExtension(tsvFile.getAbsolutePath())+"-part0.tsv";
		    try {
		        Files.move(Paths.get(tsvFile.getAbsolutePath()), Paths.get(onePart), 
		                StandardCopyOption.REPLACE_EXISTING);
		    } catch (Exception e) {Lang.error(e);}
			groupFile(m, config, onePart);
			return;
		}
		
		PrintStream[] outs = new PrintStream[numParts];
		for (int i = 0; i < outs.length; ++i)
			outs[i] = FileUtil.getFilePrintStream(FileUtil.removeExtension(tsvFile.getAbsolutePath())+"-part"+i+".tsv");
		
		for (String line : FileUtil.getLines(tsvFile.getAbsolutePath())) {
			m.fromString(line);
			int whichPart = new Random(m.groupId().hashCode()).nextInt(numParts);
			outs[whichPart].println(line);
		}
		
		for (int i = 0; i < outs.length; ++i) {
			outs[i].close();
		}
		for (int i = 0; i < outs.length; ++i) {
		    groupFile(m, config, FileUtil.removeExtension(tsvFile.getAbsolutePath())+"-part"+i+".tsv");
		}
		tsvFile.delete();
	}
	
	// if the file is too large 
	//   first split into multiple files, grouped by id-pairs 
	//    new Random(idPair.hashCode).nextInt(numSplits))
	//   then go through each file, build a map idPair->mentionSet and overwrite it
	public static void main(String[] args) {
		RelexConfig config = new RelexConfig();
		config.fromString(FileUtil.readFileAsString(args[0]));
		File tsvFile = new File(args[1]);
		
		splitAndSort(config, tsvFile);
	}
}
