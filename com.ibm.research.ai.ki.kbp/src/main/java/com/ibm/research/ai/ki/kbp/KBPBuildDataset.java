package com.ibm.research.ai.ki.kbp;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.unary.*;
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
        options.addOption("unaryConfig", true, "A RelexConfig in properties file format, for unary relation construction");   
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
        
        String inputCorpus = cmd.getOptionValue("in");
        String convertDir = cmd.getOptionValue("out");
        
        try {
            FileUtil.ensureWriteable(new File(convertDir, "typePairs.tsv"));
            Files.copy(
                    Paths.get(cmd.getOptionValue("kb"), RelexDatasetFiles.typePairFilterFile), 
                    Paths.get(convertDir, RelexDatasetFiles.typePairFilterFile), 
                    StandardCopyOption.REPLACE_EXISTING);
            Files.copy(
                    Paths.get(cmd.getOptionValue("kb"), RelexDatasetFiles.idCountsFile), 
                    Paths.get(convertDir, RelexDatasetFiles.idCountsFile), 
                    StandardCopyOption.REPLACE_EXISTING);
            Files.copy(
                    Paths.get(cmd.getOptionValue("kb"), RelexDatasetFiles.typeFilterFile), 
                    Paths.get(convertDir, RelexDatasetFiles.typeFilterFile), 
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            Lang.error(e);
        }
        
        
        Iterable<File> files = new FileUtil.FileIterable(new File(inputCorpus));
        Function<File,Iterator<Document>> f2docs = new Function<File,Iterator<Document>>() {
            @Override
            public Iterator<Document> apply(File f) {
                return Iterables.transform(FileUtil.getLines(f.getAbsolutePath()), 
                        str -> DocumentSerialize.fromString(str)).iterator();
            }
        };
        Iterable<Document> docs = new NestedIterable<File, Document>(files, f2docs);
        
        System.out.println("Beginning tsv dataset creation");
        
        if (cmd.hasOption("config")) {
            RelexConfig config = new RelexConfig();
            config.fromString(FileUtil.readFileAsString(cmd.getOptionValue("config")));
            config.groundTruthFile = new File(cmd.getOptionValue("kb"), "gt.ser.gz").getAbsolutePath();
            config.convertDir = convertDir;
            CreateTsvDataset ctd = new CreateTsvDataset((GroundTruth)config.getManager().getGroundTruth(), config);
            File unsorted = new File(new File(config.convertDir, "contextSets"), "contexts.tsv");
            ctd.process(docs, unsorted);
        }

        if (cmd.hasOption("unaryConfig")) {
            RelexConfig config = new RelexConfig();
            config.fromString(FileUtil.readFileAsString(cmd.getOptionValue("unaryConfig")));
            config.groundTruthFile = new File(cmd.getOptionValue("kb"), "ugt.ser.gz").getAbsolutePath();
            config.convertDir = convertDir;
            UnaryRelexTsvDataset utd = new UnaryRelexTsvDataset((UnaryGroundTruth)config.getManager().getGroundTruth(), config);
            File unsorted = new File(new File(config.convertDir, "unaryContextSets"), "contexts.tsv");
            utd.process(docs, unsorted);
        }
        
        //TODO: RelexVocab
        
        //TODO: RelexStats
        
        //TODO: RelexTensors
    }
}
