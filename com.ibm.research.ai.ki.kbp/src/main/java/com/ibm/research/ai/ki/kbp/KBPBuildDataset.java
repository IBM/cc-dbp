package com.ibm.research.ai.ki.kbp;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;

import org.apache.commons.cli.*;

import com.google.common.collect.*;

/**
 * Non-spark version to build tsv dataset from corpus and ground truth.
 * @author mrglass
 *
 */
public class KBPBuildDataset {
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("config", true, "A RelexConfig in properties file format");   
        options.addOption("in", true, "The input corpus");
        options.addOption("out", true, "The directory to create the dataset in");
        options.addOption("kb", true, "The kb directory, should contain gt.ser.gz and typePairs.tsv");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            Lang.error(pe);
        }
        
        String configProperties = cmd.getOptionValue("config");
        if (configProperties == null)
            throw new IllegalArgumentException("Must supply config.");
        String inputCorpus = cmd.getOptionValue("in");
        
        RelexConfig config = new RelexConfig();
        config.fromString(FileUtil.readFileAsString(configProperties));
        config.groundTruthFile = new File(cmd.getOptionValue("kb"), "gt.ser.gz").getAbsolutePath();
        config.convertDir = cmd.getOptionValue("out");
        try {
            FileUtil.ensureWriteable(new File(config.convertDir, "typePairs.tsv"));
            Files.copy(
                    Paths.get(cmd.getOptionValue("kb"), "typePairs.tsv"), 
                    Paths.get(config.convertDir, "typePairs.tsv"), 
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            Lang.error(e);
        }
        
        IRelexDatasetManager<? extends IRelexMention> rdmanager = config.getManager();
        
        CreateTsvDataset ctd = new CreateTsvDataset((GroundTruth)rdmanager.getGroundTruth(), config);
        Iterable<File> files = new FileUtil.FileIterable(new File(inputCorpus));
        Function<File,Iterator<Document>> f2docs = new Function<File,Iterator<Document>>() {
            @Override
            public Iterator<Document> apply(File f) {
                return Iterables.transform(FileUtil.getLines(f.getAbsolutePath()), 
                        str -> DocumentSerialize.fromString(str)).iterator();
            }
        };
        Iterable<Document> docs = new NestedIterable<File, Document>(files, f2docs);
        File unsorted = new File(new File(config.convertDir, "contextSets"), "contexts.tsv");
        System.out.println("Beginning tsv dataset creation");
        ctd.process(docs, unsorted);
        
        //TODO: RelexVocab
        
        //TODO: RelexStats
        
        //TODO: RelexTensors
    }
}
