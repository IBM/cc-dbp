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
package com.ibm.research.ai.ki.kbp.unary;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

public class UnaryGroundTruth implements IGroundTruth {
	private static final long serialVersionUID = 1L;

	Map<String,String[]> id2ur = new HashMap<>();
	Map<String,String> id2type = new HashMap<>();
	
	public static final String[] noRelations = new String[0];
	
	public static final String separator = "::";
	public static final String firstArgPrefix = "_"+separator;
	public static final String secondArgSuffix = separator+"_";
	
	static String encodeOtherArg(String arg) {
	    return Lang.urlEncode(arg);
	}
	static String decodeOtherArg(String arg) {
	    return Lang.urlDecode(arg);
	}
	
	
	public static String toUnaryRelation(String[] trip, int forArg) {	    
	    if (forArg != 0 && forArg != 2)
	        throw new IllegalArgumentException("arg must be 0 or 2");
	    String rel = trip[1];
	    if (rel.contains("\t") || rel.contains("\n") || rel.contains("::") || rel.contains(","))
	        throw new IllegalArgumentException("Unary relation contains illegal character(s)");
	    if (forArg == 0)
	        return firstArgPrefix+rel+separator+encodeOtherArg(trip[2]);
	    if (forArg == 2)
            return encodeOtherArg(trip[0])+separator+rel+secondArgSuffix;
	    throw new IllegalStateException();
	}
	
	public static boolean isValidUnaryRelation(String urel) {
	    if (urel.contains("\t") || urel.contains("\n") || urel.contains(","))
	        return false;
	    if (urel.startsWith(firstArgPrefix)) {
	        int relEnd = urel.indexOf(separator, firstArgPrefix.length());
	        if (relEnd == -1 || relEnd == urel.length()-separator.length() || relEnd == firstArgPrefix.length())
	            return false;
	        return true;
	    } else if (urel.endsWith(secondArgSuffix)) {
	        int relStart = urel.lastIndexOf(separator, urel.length()-secondArgSuffix.length());
	        if (relStart == -1 || relStart == 0)
	            return false;
	        return true;
	    } else {
	        return false;
	    }
	}
	
	public static String[] toBinaryRelation(String unaryRelation, String arg) {
	    String rname = unaryRelation;
	    String[] trip = new String[3];
	    if (rname.startsWith(firstArgPrefix)) {
            int relEnd = rname.indexOf(separator, firstArgPrefix.length());
            String otherArg = decodeOtherArg(rname.substring(relEnd+separator.length()));
            trip[0] = arg;
            trip[1] = rname.substring(firstArgPrefix.length(), relEnd);
            trip[2] = otherArg;
        } else if (rname.endsWith(secondArgSuffix)) {
            int relStart = rname.lastIndexOf(separator, rname.length()-secondArgSuffix.length())+separator.length();
            String otherArg = decodeOtherArg(rname.substring(0, relStart-separator.length()));
            trip[0] = otherArg;
            trip[1] = rname.substring(relStart, rname.length()-secondArgSuffix.length());
            trip[2] = arg;
        }
	    return trip;
	}
	
	public Set<String> getRelevantIds() {
		Set<String> ids = new HashSet<>();
		if (id2type != null)
			ids.addAll(id2type.keySet());
		ids.addAll(id2ur.keySet());
		return ids;
	}
	
	public String getType(String id) {
		if (id2type == null)
			return null;
		return id2type.get(id);
	}
	
	public Map<String,String> getTypeMap() {
		return id2type;
	}
	
	/**
	 * Should we return null for id not in idsOfInterset
	 * @param id
	 * @return
	 */
	public String[] getRelations(String id) {
		return Lang.NVL(id2ur.get(id), noRelations);
	}
	
	@Override
	public Map<String,String[]> buildEntitySetId2Relations() {
		return id2ur;
	}

	/**
	 * Build ground truth
	 * @param instance2Rels
	 * @param id2type may be null
	 * @return
	 */
	public static <T extends Collection<String>> UnaryGroundTruth build(Map<String, T> instance2Rels, Map<String,String> id2type) {
		UnaryGroundTruth ugt = new UnaryGroundTruth();
		for (Map.Entry<String, T> e : instance2Rels.entrySet()) {
			String[] types = e.getValue().isEmpty() ? noRelations : e.getValue().toArray(noRelations);
			//check format of the relation string. it should be either _::relationType::fixedArg2 or fixedArg1::relationType::_
			for (String t : types)
			    if (!isValidUnaryRelation(t))
			        throw new IllegalArgumentException("Badly formated relation name: "+t);
			ugt.id2ur.put(e.getKey(), types);
		}
		ugt.id2type = id2type;
		return ugt;
	}
	
}
