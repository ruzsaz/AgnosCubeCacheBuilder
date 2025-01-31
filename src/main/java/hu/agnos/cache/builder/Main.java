package hu.agnos.cache.builder;

import hu.agnos.cube.CountDistinctCube;
import hu.agnos.cube.Cube;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        Options options = new Options();

        Option inputFileOption = new Option("i", "input", true, "input cube file path");
        inputFileOption.setRequired(true);
        options.addOption(inputFileOption);

        Option complexityOption = new Option("c", "complexity", true, "complexity threshold");
        complexityOption.setRequired(true);
        complexityOption.setType(Integer.class);
        options.addOption(complexityOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            formatter.printHelp("Agnos cube cache builder", options);
            Runtime.getRuntime().exit(1);
        }

        String inputFilePath = cmd.getOptionValue("input");
        int complexity = Integer.parseInt(cmd.getOptionValue("complexity"));
        String outputFilePath = inputFilePath.substring(0, inputFilePath.lastIndexOf('.')) + "_cache" + complexity + ".cube";


        Optional<Cube> optCube = Main.loadFileAsCube(inputFilePath);
        if (optCube.isPresent()) {
            Cube cube = optCube.get();
            CacheCreator creator = new CacheCreator(cube);
            creator.createCache(complexity);
            Main.saveCube(cube, outputFilePath);
        } else {
            log.error("Cube loading failed from file: {}", inputFilePath);
        }
    }

    private static void saveCube(Cube cube, String path) {
        System.out.println("Saving cube to file: " + path);
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(cube);
            out.close();
            fileOut.close();
            System.out.println("Cube is saved, exiting");
            System.out.println(cube.getCacheSize());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static Optional<Cube> loadFileAsCube(String path) {
        System.out.println("Loading cube from file: " + path);
        File file = new File(path);

        Cube result = null;
        try (FileInputStream fileIn = new FileInputStream(file); ObjectInput in = new ObjectInputStream(fileIn)) {
            Object o = in.readObject();
            if (o.getClass().equals(CountDistinctCube.class )){
                CountDistinctCube cube = (CountDistinctCube) o;
                cube.initCountDistinctCube();
                result = cube;
            }
            else {
                result = (Cube) o;
            }
            System.out.println("Cube data loaded");
        } catch (IOException | ClassNotFoundException ex) {
            log.error("Cube loading failed from file: {}", file.getName(), ex);
        }
        return Optional.ofNullable(result);
    }
}