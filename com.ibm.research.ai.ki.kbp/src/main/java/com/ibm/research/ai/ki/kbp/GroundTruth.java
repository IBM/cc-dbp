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
import java.util.function.*;

import com.ibm.research.ai.ki.util.*;

import com.google.common.collect.*;

public class GroundTruth implements IGroundTruth {
	private static final long serialVersionUID = 1L;
	
	public GroundTruth(Set<String> typesOfInterest, Set<String> relationsOfInterest) {
		this.relationsOfInterest = relationsOfInterest;
		this.typesOfInterest = typesOfInterest;
	}
	
	public GroundTruth() {}
	
	//these relations don't have a direction indicator
	public Set<String> relationsOfInterest;
	public Set<String> typesOfInterest;
	
	protected Map<String, HashSet<String>> instance2Type = new HashMap<>();
	protected Map<Pair<String,String>, ArrayList<String>> instancePair2Relation = new HashMap<>();
	
	public static final String forwardRelationPrefix = ">";
	public static final String inverseRelationPrefix = "<";
	
	public static final String unknownType = "unk";
	public static final String multipleType = "multiType";
	
	public static String unorderedRelation(String relType) {
	    if (relType.startsWith(forwardRelationPrefix) || relType.startsWith(inverseRelationPrefix))
	        return relType.substring(1);
	    else
	        return relType;
	}
	
	@Override
	public Map<String,String[]> buildEntitySetId2Relations() {
		Map<String,ArrayList<String>> entSet2Rels = new HashMap<>();
		for (Map.Entry<Pair<String,String>, ArrayList<String>> e : instancePair2Relation.entrySet()) {
			ArrayList<String> rels = entSet2Rels.computeIfAbsent(
					GroundTruth.getOrderedIdPair(e.getKey().first, e.getKey().second), 
					k -> new ArrayList<>());
			for (String r : e.getValue())
			    rels.add(unorderedRelation(r));
		}
		
		return new HashMap<>(Maps.transformValues(entSet2Rels, a -> a.toArray(new String[a.size()])));
	}
	
	/**
	 * limit this ground truth to a subset of the relations
	 * @param relations
	 */
	public void setRelationSubset(Collection<String> relationSubset) {
	    Set<String> relations = new HashSet<>(relationSubset);
	    relationsOfInterest.retainAll(relations);
	    Iterator<Map.Entry<Pair<String,String>, ArrayList<String>>> it = instancePair2Relation.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<Pair<String,String>, ArrayList<String>> e = it.next();
	        e.getValue().retainAll(relations);
	        if (e.getValue().isEmpty())
	            it.remove();
	    }
	}
	
	/**
	 * Gets the urls that participate in a relation in relationsOfInterest or have a type in typesOfInterest.
	 * @return
	 */
	public Set<String> getRelevantIds() {
		Set<String> ru = new HashSet<>();
		ru.addAll(instance2Type.keySet());
		for (Pair<String,String> idP : instancePair2Relation.keySet()) {
			ru.add(idP.first);
			ru.add(idP.second);
		}
		return ru;
	}
	
	/**
	 * Check if this ground truth will support a RelexConfig.directionStyle of fixed
	 * @return
	 */
	public List<String> canBeFixedRelationDirection() {
		//return relations that violate the direction
		List<String> violations = new ArrayList<>();
		for (Pair<String,String> ep : instancePair2Relation.keySet()) {
			if (instancePair2Relation.containsKey(Pair.of(ep.second, ep.first))) {
				String v = ep.first + " and " + ep.second + "\n"+
						"  > " + Lang.stringList(instancePair2Relation.get(ep), ", ") + "\n" +
						"  < " + Lang.stringList(instancePair2Relation.get(Pair.of(ep.second, ep.first)), ", ");
				violations.add(v);
			}
		}
		return violations;
	}
	
	/**
	 * Get the number of triples for each relation that are present in the data split [rstart,rend) but not found in entPairsSeen.
	 * @param rstart
	 * @param rend
	 * @param entPairsSeen
	 * @return
	 */
	public Map<String,MutableDouble> getOutOfRecall(double rstart, double rend, Iterator<Pair<String,String>> entPairsSeen) {
		Map<String,MutableDouble> relCounts = new HashMap<>();
		
		Set<String> entPairIdsSeen = new HashSet<>();
		while (entPairsSeen.hasNext()) {
			Pair<String,String> ep = entPairsSeen.next();
			entPairIdsSeen.add(getOrderedIdPair(ep.first, ep.second));
		}

		//go through the instancePair2Relation keySet
		//  find the ones in this split range not present in entPairsSeen
		//  count the number of instances for each relation
		for (Map.Entry<Pair<String,String>,ArrayList<String>> e : instancePair2Relation.entrySet()) {
			Pair<String,String> ip = e.getKey();
			double splitPoint = getSplitLocation(ip.first, ip.second);
			if (splitPoint >= rstart && splitPoint < rend) {
				if (!entPairIdsSeen.contains(getOrderedIdPair(ip.first, ip.second))) {
					for (String r : e.getValue())
						SparseVectors.increase(relCounts, r, 1.0);
				}
			}
		}
		
		return relCounts;
	}
	
	public static String getOrderedIdPair(String id1, String id2) {
		String idPair = null;
		if (id1.compareTo(id2) > 0) {
			idPair = id2+"\t"+id1;
		} else {
			idPair = id1+"\t"+id2;
		}
		return idPair;
	}
	
	/**
	 * When we downsample documents, we discard them if their downsample priority is higher than our documentSamplePercent.
	 * @param id1
	 * @param id2
	 * @return
	 */
	public static double getDocumentDownsamplePriority(String textOrId) {
	    return RandomUtil.pseudoRandomFromString("SAMPLEDOC"+textOrId);
		//return new Random(
		//		555555555+
		//		textOrId.hashCode()).nextDouble();
	}
	
	/**
	 * The train/validate/split is a division on the [0,1) range.
	 * For example [0,0.8) in train, [0.8,0.9) in validate and [0.9,1.0) in test.
	 * This function locates the provided id pair in that range, determining which data split it goes in.
	 * @param id1 one of the entity ids (order irrelevant)
	 * @param id2 the other entity id 
	 * @return
	 */
	public static double getSplitLocation(String id1, String id2) {
	    return RandomUtil.pseudoRandomFromString("SPLIT"+getOrderedIdPair(id1,id2));
		//return new Random(
		//		Double.doubleToLongBits(Math.E)+
		//		getOrderedIdPair(id1,id2).hashCode()).nextDouble();
	}

	/**
	 * The train/validate/split is a division on the [0,1) range.
	 * For example [0,0.8) in train, [0.8,0.9) in validate and [0.9,1.0) in test.
	 * This function locates the provided id pair in that range, determining which data split it goes in.
	 * @param ids the entity ids (order irrelevant) 
	 * @return
	 */
	public static double getSplitLocation(String ids) {
		int tab = ids.indexOf('\t');
		if (tab == -1)
			return getSingleSplitLocation(ids);
		String id1 = ids.substring(0, tab);
		String id2 = ids.substring(tab+1);
		return getSplitLocation(id1,id2);
	}
	
	/**
	 * When we downsample negative id pairs, we discard them if their downsample priority is higher than our negativeSampleProbability.
	 * @param id1
	 * @param id2
	 * @return
	 */
	public static double getDownsamplePriority(String id1, String id2) {
	    return RandomUtil.pseudoRandomFromString("DOWNSAMPLEID"+getOrderedIdPair(id1,id2));
	    //old style:
		//return new Random(
		//		Double.doubleToLongBits(Math.PI)+
		//		getOrderedIdPair(id1,id2).hashCode()).nextDouble();
	}
	
	/**
	 * When we downsample negative id pairs, we discard them if their downsample priority is higher than our negativeSampleProbability.
	 * @param id1
	 * @param id2
	 * @return
	 */
	public static double getDownsamplePriority(String ids) {
		int tab = ids.indexOf('\t');
		if (tab == -1)
			return getSingleDownsamplePriority(ids);
		String id1 = ids.substring(0, tab);
		String id2 = ids.substring(tab+1);
		return getDownsamplePriority(id1,id2);
	}
	
	public static double getSingleDownsamplePriority(String oneId) {
	    return RandomUtil.pseudoRandomFromString("DOWNSAMPLEID"+oneId);
		//return new Random(
		//		Double.doubleToLongBits(Math.PI)+
		//		oneId.hashCode()).nextDouble();
	}
	public static double getSingleSplitLocation(String oneId) {
	    return RandomUtil.pseudoRandomFromString("SPLIT"+oneId);
		//return new Random(
		//		Double.doubleToLongBits(Math.E)+
		//		oneId.hashCode()).nextDouble(); //could use a better hashcode here, maybe MessageDigest
	}
	
	public static double[] dataRangeFromString(String drange) {
		Objects.requireNonNull(drange);
		double[] r = new double[2];
		if (drange.startsWith("(") || drange.startsWith("["))
			drange = drange.substring(1);
		if (drange.endsWith(")") || drange.endsWith("]"))
			drange = drange.substring(0, drange.length()-1);
		int comma = drange.indexOf(',');
		r[0] = Double.parseDouble(drange.substring(0, comma).trim());
		r[1] = Double.parseDouble(drange.substring(comma+1).trim());
		return r;
	}
	
	/**
	 * Add (one of) the type(s) for id.
	 * @param id
	 * @param type
	 */
	public void addType(String id, String type) {
		HashMapUtil.addHS(instance2Type, id, type);
	}
	
	/**
	 * Adds the relation between arg1 and arg2. Relations should only be added one way without the direction indicator (arg1 relation arg2). 
	 * The inverse relation is coded by a '<' prefix rather than '>'.
	 * @param arg1
	 * @param arg2
	 * @param relation
	 */
	public void addRelation(String arg1, String arg2, String relation) {
	    if (relation.contains(",") || relation.contains("\t") || relation.contains("\n"))
	        throw new IllegalArgumentException(relation+" contains illegal characters");
	    if (relation.startsWith(forwardRelationPrefix) || relation.startsWith(inverseRelationPrefix))
	        throw new IllegalArgumentException("Direction indication ('<' or '>') is reserved for internal use.");
		//CONSIDER: check that we don't add a duplicate
		HashMapUtil.addAL(instancePair2Relation, Pair.of(arg1, arg2), relation);
	}
	

	
	//CONSIDER: report on distribution of number of types
	
	/**
	 * gets the type of interest for the given url, returns null if the url is not a type of interest.
	 * @param url
	 * @return
	 */
	public String getType(String url) {
		Collection<String> types = instance2Type.get(url);
		if (types == null)
			return null;
			//throw new IllegalArgumentException("Unknown url: "+url);
		if (typesOfInterest == null) {
			if (types.size() > 1)
				return multipleType;
			return types.iterator().next();
		}
		for (String type : types) {
			if (typesOfInterest.contains(type))
				return type;
		}
		return null;
	}
	
	public Map<String,String> getTypeMap() {
		Map<String,String> tm = new HashMap<>();
		for (String inst : instance2Type.keySet())
			tm.put(inst, getType(inst));
		return tm;
	}
	
	public String reportMentions(Function<String,Integer> idPairMentionCount) {
		double[] thresholds = new double[] {1, 2, 3, 4, 5, 10, 20, 50, 100};
		int[] counts = new int[thresholds.length+1];
		for (Pair<String,String> idpair : instancePair2Relation.keySet()) {
			int mcount = idPairMentionCount.apply(idpair.first+"\t"+idpair.second) + idPairMentionCount.apply(idpair.second+"\t"+idpair.first);
			for (int i = 0; i < counts.length; ++i) {
				if (i == thresholds.length || mcount < thresholds[i]) {
					++counts[i];
					break;
				}
			}
		}
		return Distribution.stringHisto(thresholds, counts);
	}
	
	/**
	 * 
	 * @param url1
	 * @param url2
	 * @return relations prefixed with '>' for forward and with '<' for backward
	 */
	public Collection<String> getRelations(String url1, String url2) {
		//so we look up in instance2Type using both orders
		List<String> forward = instancePair2Relation.get(Pair.of(url1, url2));
		List<String> backward = instancePair2Relation.get(Pair.of(url2, url1));
		if (forward == null && backward == null)
			return Collections.EMPTY_LIST;
		List<String> all = new ArrayList<>();
		if (forward != null)
			for (String r : forward)
				all.add(forwardRelationPrefix+r);
		if (backward != null)
			for (String r : backward)
				all.add(inverseRelationPrefix+r);
		return all;
	}
	/**
	 * 
	 * @param url
	 * @return
	 */
	public Collection<String> getTypes(String url) {
		return Lang.NVL(instance2Type.get(url), Collections.EMPTY_LIST);
	}
	
}
