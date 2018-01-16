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
package com.ibm.research.ai.ki.kb.conversion;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kb.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.graphs.*;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.*;

import com.google.common.collect.*;

public class ConvertDBpedia {
    
    //namespaces: dbo: dbl: (literals) uri: (non dbo uris)
    
    /**
     * Finds the instances that the gazetteer should match in a case sensitive way.
     * Done right after making types.tsv
     * @param kbDir
     */
    static void writeCaseSensitivity(File kbDir) {
        Set<String> csTypes = new HashSet<>(Arrays.asList("dbo:Work", "dbo:Person", "dbo:Agent", "dbo:Place"));
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDir, "caseSensitive.tsv").getAbsolutePath())) {
            for (String[] typ : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
                for (int i = 1; i < typ.length; ++i) {
                    if (csTypes.contains(typ[i])) {
                        out.println(typ[0]);
                        break;
                    }
                }
            }
        }
    }
    
    static void caseSensitiveTypes(String kbDir) {
        Map<String, String[]> id2types = new HashMap<>();
        for (String[] typ : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
            id2types.put(typ[0], Arrays.copyOfRange(typ, 1, typ.length));
        }
        Map<String,MutableDouble> typeCount = new HashMap<>();
        Map<String,MutableDouble> typeCountCaseSensitive = new HashMap<>();
        for (String[] lbl : new SimpleTsvIterable(new File(kbDir, KBFiles.labelsTsv))) {
            boolean caseSensitive = false;
            boolean multiToken = false;
            for (int i = 1; i < lbl.length; ++i) {
                // multiple tokens not all lowercase
                String[] toks = lbl[i].split("\\s+");
                if (toks.length > 1) {
                    multiToken = true;
                    for (int j = 1; j < toks.length; ++j)
                        if (!toks[j].toLowerCase().equals(toks[j]))
                            caseSensitive = true;
                    break;
                }
            }
            if (multiToken) {
                //get types for id
                String[] types = id2types.get(lbl[0]);
                if (types != null) {
                    for (String type : types) {
                        SparseVectors.increase(typeCount, type, 1);
                        if (caseSensitive)
                            SparseVectors.increase(typeCountCaseSensitive, type, 1);
                    }
                }
            }
        }
        SparseVectors.divideBy(typeCountCaseSensitive, typeCount);
        System.out.println(SparseVectors.toString(typeCountCaseSensitive, 100));
    }
    
    //can be done right after downloading dbpedia
    static void createTypesTsv(DBpediaKBConfig config) {
        File kbDir = config.kbDir();
        System.out.println("Creating types.tsv");
        Set<String> ids = new HashSet<>();
        if (new File(kbDir, KBFiles.idCountsTsv).exists()) {
            System.out.println("Using "+KBFiles.idCountsTsv);
            for (String line : FileUtil.getLines(new File(kbDir, KBFiles.idCountsTsv).getAbsolutePath())) {
                int space = Math.max(line.lastIndexOf(' '), line.lastIndexOf('\t'));
                ids.add(line.substring(0,space).trim());
            }
        } else {
            for (String[] trip : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
                ids.add(trip[0]);
                ids.add(trip[1]);
            }
        }
        //CONSIDER: This chews a lot of memory - maybe use a fastutil map?
        Map<String,HashSet<String>> type2insts = new HashMap<>();
        for (String[] t : NTriples.getProperties(config.typesFile())) {
            String id = t[0];
            id = useNamespacePrefix(id);
            if (ids.contains(id)) {
                String type = useNamespacePrefix(t[2]);
                if (type.startsWith("dbo:") && !type.contains("Wikidata"))
                    HashMapUtil.addHS(type2insts, type, id);
            }
        }
        
        SelectTypes selectTypes = new SelectTypes(type2insts, config.minTypeSize, config.maxNumberOfTypes);
        selectTypes.verbose = true;
        Set<String> selectedTypes = new HashSet<>(selectTypes.computeSimple());
        Map<String,Integer> typesByFreq = HashMapUtil.arrayToIndex(selectTypes.getTypesByFreq());
        Map<String,HashSet<String>> inst2types = HashMapUtil.reverseHS(type2insts);
        //write types.tsv (types ordered most specific first)
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDir, KBFiles.typesTsv).getAbsolutePath())) {
            for (Map.Entry<String, HashSet<String>> e : inst2types.entrySet()) {
                Set<String> selected = Sets.intersection(e.getValue(), selectedTypes);
                List<Pair<String,Integer>> selectedSorted = new ArrayList<>();
                for (String s : selected) {
                    selectedSorted.add(Pair.of(s, typesByFreq.get(s)));
                }
                SecondPairComparator.sort(selectedSorted);
                if (!selectedSorted.isEmpty()) {
                    out.println(e.getKey()+"\t"+Lang.stringList(Iterables.transform(selectedSorted, p -> p.first), "\t"));
                }
            }
            // types for literals too
            for (String id : ids) {
                if (id.startsWith("dbl:")) {
                    if (Lang.isInteger(id.substring("dbl:".length()))) {
                        out.println(id+"\t"+"literal:number");
                    } else {
                        out.println(id+"\t"+"literal:string");
                    }
                }
            }
            
        }
    }
    
    public static String useNamespacePrefix(String uri) {
        if (uri.startsWith("http://dbpedia.org/ontology/")) {
            uri = "dbo:"+uri.substring("http://dbpedia.org/ontology/".length());
        } else if (uri.startsWith("http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#")) {
            uri = "odp:"+uri.substring("http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#".length());
        } else if (uri.startsWith("http://dbpedia.org/resource/")) {
            uri = "dbr:"+uri.substring("http://dbpedia.org/resource/".length());
        } else if (uri.startsWith("http://schema.org/")) {
            uri = "sco:"+uri.substring("http://schema.org/".length());
        } else if (uri.startsWith("http://www.wikidata.org/entity/")) {
            uri = "wde:"+uri.substring("http://www.wikidata.org/entity/".length());
        } else if (uri.startsWith("http://xmlns.com/foaf/0.1/")) {
            uri = "xff:"+uri.substring("http://xmlns.com/foaf/0.1/".length());
        }
        return uri;
    }
    
    /**
     * Gets a type to direct subtype map. Used in (at least) SummaryCharts for the entity type sunburst chart.
     * @param dbpediaOwl
     * @return
     */
    public static Map<String,Set<String>> entityTypeHierarchy(String dbpediaOwl) {
        Map<String,Set<String>> type2directSubtype = new HashMap<>();
        Document doc = XmlTag.xmlToOffsetAnnotation(FileUtil.readFileAsString(dbpediaOwl), true);
        for (XmlTag t : doc.getAnnotations(XmlTag.class)) {
            if (t.name.equals("owl:Class")) {
                String type = useNamespacePrefix(t.attributes.get("rdf:about"));
                for (XmlTag ti : t.getChildren()) {
                    if (ti.name.equals("rdfs:subClassOf")) {
                        String sup = useNamespacePrefix(ti.attributes.get("rdf:resource"));
                        type2directSubtype.computeIfAbsent(sup, k->new HashSet<>()).add(type);
                    }
                }
            }
        }
        return type2directSubtype;
    }
    
    public static String date2year(String rel) {
        if (rel.endsWith("Date"))
            rel = rel.substring(0, rel.length()-4) + "Year";
        return rel;
    }
    
    public static Map<String,Set<String>> relation2DirectSubRelation(String dbpediaOwl) {
        Map<String,Set<String>> type2directSubtype = new HashMap<>();
        Document doc = XmlTag.xmlToOffsetAnnotation(FileUtil.readFileAsString(dbpediaOwl), true);
        for (XmlTag t : doc.getAnnotations(XmlTag.class)) {
            if (t.name.equals("owl:ObjectProperty")) {
                String type = useNamespacePrefix(t.attributes.get("rdf:about"));
                type = date2year(type);
                for (XmlTag ti : t.getChildren()) {
                    if (ti.name.equals("rdfs:subPropertyOf")) {
                        String sup = useNamespacePrefix(ti.attributes.get("rdf:resource"));
                        sup = date2year(sup);
                        type2directSubtype.computeIfAbsent(sup, k->new HashSet<>()).add(type);
                    }
                }
            }
        }
        return type2directSubtype;
    }
    
    /**
     * Relation to super-relation from the DBpedia owl file.
     * @return
     */
    public static Map<String,Set<String>> relationTypeHierarchy(File dbpediaOwl) {
        Map<String,HashSet<String>> rel2sup = new HashMap<>();
        Set<String> seenRelations = new HashSet<>();
        Document doc = XmlTag.xmlToOffsetAnnotation(FileUtil.readFileAsString(dbpediaOwl), true);
        for (XmlTag t : doc.getAnnotations(XmlTag.class)) {
            if (t.name.equals("owl:ObjectProperty")) {
                String relName = date2year(useNamespacePrefix(t.attributes.get("rdf:about")));
                //use prefixes - dbo:, odp:,
                
                String superRel = null;
                for (XmlTag ti : t.getChildren()) {
                    if (ti.name.equals("rdfs:subPropertyOf")) {
                        //if (superRel != null)
                        //  throw new Error("Duplicate super rel "+relName);
                        superRel = date2year(useNamespacePrefix(ti.attributes.get("rdf:resource")));
                        HashMapUtil.addHS(rel2sup, relName, superRel);
                        //System.out.println(relName +" -> "+superRel);
                    }
                }
                seenRelations.add(relName);
            }
        }
        Map<String, Set<String>> rel2sups = GraphAlgorithms.transitiveClosure(
                rel2sup.keySet(), 
                r -> Lang.NVL(rel2sup.get(r), (Set<String>)Collections.EMPTY_SET));
        
        return rel2sups;
    }
    

    //can be done right after annotating relationSample.txt
    private static void relationTaxonomy(DBpediaKBConfig config) {
        File kbDir = config.kbDir();
        Map<String,Set<String>> rel2sup = relationTypeHierarchy(config.dbpediaOwlFile());
        Set<String> goodRelations = SelectRelations.getSelectedRelations();
        //for (Map.Entry<String, HashSet<String>> e : rel2sup.entrySet()) {
        //    System.out.println(e.getKey()+"\n   "+Lang.stringList(e.getValue(), "\n   "));
        //}
        //System.out.println("=========================================");
        /*
        for (String relName : goodRelations) {
            relName = date2year(useNamespacePrefix(relName));

            Set<String> sups = rel2sup.get(relName);
            if (sups != null && !sups.isEmpty())
                System.out.println(relName+"\n   "+Lang.stringList(sups, "\n   "));
        }
        */
        //create a file to express the relation taxonomy
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDir, KBFiles.relationTaxonomyTsv).getAbsolutePath())) {
            for (Map.Entry<String, Set<String>> e : rel2sup.entrySet()) {
                out.println(e.getKey()+"\t"+Lang.stringList(e.getValue(), "\t"));
            }
        }
        
    }
    
    static Pattern pxPat = Pattern.compile("^[0-9]+px");
    private static String cleanStringLiteral(String obj) {
        obj = StringEscapeUtils.unescapeJava(obj.substring(1, obj.lastIndexOf('"'))).replace("&minus;", "-");
        if (obj.endsWith(":")) 
            return null;

        if (obj.indexOf('(') != -1) {
            obj = obj.substring(0, obj.indexOf('(')).trim();
        }
        obj = obj.replaceAll("[\\s_\\\\\"]+", " ").toLowerCase().trim().replace(' ', '_');
        //strip leading and trailing punctuation
        while (obj.length() > 1 && !Character.isLetterOrDigit(obj.charAt(0)) && obj.charAt(0) != '+' && obj.charAt(0) != '-')
            obj = obj.substring(1);
        while (obj.length() > 1 && !Character.isLetterOrDigit(obj.charAt(obj.length()-1)))
            obj = obj.substring(0, obj.length()-1);
        Matcher m = pxPat.matcher(obj);
        while (m.find()) {
            obj = obj.substring(m.end());
            m = pxPat.matcher(obj);
        }
        if (obj.replaceAll("\\W+", "").isEmpty() || obj.length() > 40) {
            return null;
        }
        return obj;

    }
    
    
    private static void makeKBFiles(DBpediaKBConfig config) {
        File kbDir = config.kbDir();
        System.out.println("Building initial KB files");
        PrintStream triplesOut = FileUtil.getFilePrintStream(new File(kbDir, KBFiles.triplesTsv).getAbsolutePath());
        PrintStream labelsOut = FileUtil.getFilePrintStream(new File(kbDir, KBFiles.labelsTsv).getAbsolutePath());
        
        Set<String> goodRelations = SelectRelations.getSelectedRelations();
        //System.out.println("Good:\n   "+Lang.stringList(goodRelations, "\n   "));
        
        Set<String> dbrs = new HashSet<>();
        Set<String> dbls = new HashSet<>();
        String prevSubj = "";
        Set<String> noDupTriples = new HashSet<String>();
        //iterate over mappingbased* and write triples
        //  while building set of dbo: and write out dbl: and uri:
        for (File f : new File[] {config.objectsFile(), config.labelsFile()}) {
            for (String[] trip : NTriples.getProperties(f)) {
                if (goodRelations.contains(trip[1])) {
                    String subj = trip[0];
                    if (!subj.equals(prevSubj)) {
                        noDupTriples.clear();
                        prevSubj = subj;
                    }
                    String obj = trip[2];
                    String rel = trip[1];
                    //we are taking only the year portion of the date
                    rel = date2year(rel);
                    if (rel.startsWith("http://dbpedia.org/ontology/")) {
                        rel = "dbo:"+rel.substring("http://dbpedia.org/ontology/".length());
                    }
                    //skip mediators
                    if (subj.indexOf("__") != -1 || obj.indexOf("__") != -1)
                        continue;
                    if (subj.startsWith("http://dbpedia.org/resource/")) {
                        subj = "dbr:"+subj.substring("http://dbpedia.org/resource/".length());
                        dbrs.add(subj);
                    } else {
                        System.err.println("Unexpected subject: "+subj);
                        continue;
                    }
                    
                    //CONSIDER: if the relation is http://xmlns.com/foaf/0.1/nick then split on ',' and ';'
                    
                    if (obj.startsWith("http://dbpedia.org/resource/")) {
                        obj = "dbr:"+obj.substring("http://dbpedia.org/resource/".length());
                        dbrs.add(obj);
                    } else if (obj.startsWith("\"")) {
                        List<String> labels = new ArrayList<>();
                        if (obj.endsWith("^^<http://www.w3.org/2001/XMLSchema#date>") || obj.endsWith("^^<http://www.w3.org/2001/XMLSchema#gYear>")) {
                            int end = obj.indexOf('-', 2);
                            if (end == -1)
                                end = obj.lastIndexOf('"');
                            obj = obj.substring(1, end);
                            if (obj.startsWith("-")) {
                                obj = obj.substring(1);
                                while (obj.startsWith("0"))
                                    obj = obj.substring(1);
                                if (!Lang.isInteger(obj))
                                    obj = null;
                                
                                labels.add(obj+" BC");
                                labels.add(obj+" BCE");
                                obj = obj + "_BCE";
                            } else {
                                while (obj.startsWith("0") && obj.length() > 1)
                                    obj = obj.substring(1);
                                labels.add(obj);
                                if (!Lang.isInteger(obj)) {
                                    obj = null;
                                } else if (Integer.parseInt(obj) < 1500) {
                                    //labels.add(obj+" AD");
                                    //labels.add(obj+" CE");
                                }
                            }
                        } else {
                            obj = cleanStringLiteral(obj);
                            if (obj != null) {
                                labels.add(obj);
                            }
                        }
                        
                        if (obj != null) {
                            obj = "dbl:"+obj;
                            if (dbls.add(obj))
                                labelsOut.println(obj+"\t"+Lang.stringList(labels, "\t"));
                        }
                    } else {
                        //it is a non-dbo uri
                        String label = obj;
                        obj = "dbl:"+obj;
                        if (dbls.add(obj)) {
                            labelsOut.println(obj+"\t"+label);
                        }
                    }
                    if (obj == null) {
                       continue;
                    }

                    String triple = subj+"\t"+rel+"\t"+obj;
                    if (noDupTriples.add(triple))
                        triplesOut.println(triple);
                }
            }
        }
        dbls.clear();
        triplesOut.close();
        
        //check on what (if any) dbo: do not have labels
        //check on multiple labels
        //CONSIDER: look in mappingbased literals for label relations
        //open up labels and write dbo: labels
        Map<String,MutableDouble> labelCounts = new HashMap<>();
        for (String[] trip : NTriples.getProperties(config.labelsFile())) {
            String subj = trip[0];
            if (subj.startsWith("http://dbpedia.org/resource/")) {
                subj = "dbr:"+subj.substring("http://dbpedia.org/resource/".length());
                if (dbrs.contains(subj)) {
                    SparseVectors.increase(labelCounts, subj, 1.0);
                    labelsOut.println(subj+"\t"+NTriples.getStringLiteral(trip[2]));
                }
            }
        }
        System.out.println("No labels for: "+(dbrs.size()-labelCounts.size())+" creating labels from URIs.");
        for (String dbr : dbrs) {
            if (!labelCounts.containsKey(dbr)) {
                labelsOut.println(dbr+"\t"+dbrUri2Label(dbr));
            }
        }
        labelsOut.close();

        //FileUtil.writeFileAsString("nolabels.txt", Lang.stringList(Sets.difference(dbrs, labelCounts.keySet()), "\n"));
        SparseVectors.trimByThreshold(labelCounts, 2.0);
        System.out.println("Duplicates labels: "+labelCounts.size());
    }
    
    private static String dbrUri2Label(String dbr) {
        String uri = dbr.substring("dbr:".length());
        int paren = uri.indexOf('(');
        if (paren != -1)
            uri = uri.substring(0, paren);
        return Lang.urlDecode(uri.replace('_', ' ').trim());
    }
    
    //CONSIDER: improve handling of list valued literal attributes
    
    /**
     * Example args:
       -config dbpediaConfig.properties
       -kb /my/local/dir/dbpediaKB
     * @param args
     */
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("config", true, "A DBpediaKBConfig in properties file format");   
        options.addOption("kb", true, "The kb directory to create or add to");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            Lang.error(pe);
        }
        
        String configProperties = cmd.getOptionValue("config");
        
        DBpediaKBConfig config = new DBpediaKBConfig();
        config.fromProperties(PropertyLoader.loadProperties(configProperties));
        config.kbDir = cmd.getOptionValue("kb");
        
        File dbpediaDir = config.kbDir();
        if (!new File(dbpediaDir, KBFiles.triplesTsv).exists()) {
            SelectRelations.downloadDBpedia(config); //download dbpedia files if not present
            makeKBFiles(config);    
            relationTaxonomy(config);
            //NOTE: the minCount and maxCount don't do anything unless idCounts.tsv exists
            if (!config.noNodeCorpusCounts)
                BuildGazetteer.build(dbpediaDir, config.minNodeCorpusCount, config.maxNodeCorpusCount);
        }
        
        if (config.noNodeCorpusCounts || new File(dbpediaDir, KBFiles.idCountsTsv).exists()) {
            createTypesTsv(config); 
            MergeNodesDBpedia.merge(dbpediaDir); 
            writeCaseSensitivity(dbpediaDir); 
            
            BuildGazetteer.build(dbpediaDir, config.minNodeCorpusCount, config.maxNodeCorpusCount);
            BuildGroundTruth.build(dbpediaDir, config.minNodeCorpusCount, config.minUnaryCount, config.useRelationTaxonomy);
            TypePairFilter.binary(dbpediaDir, config.minTypePairFreq);
            TypePairFilter.unary(dbpediaDir, config.minTypeFreqForUnary);
        }
    }
}
