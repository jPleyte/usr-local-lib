package io.github.jpleyte.vcf.detail;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.StopWatch;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import io.github.jpleyte.log.BootstrapLogger;

/**
 * Analyse a VCF by running a set of functions on each line.
 *
 * To Do: - 
 * - [ ] Try using HTSLib instead of HtsJdk 
 * - [x] Use multiple threads 
 * - [ ] Add support for file input via stdio 
 * - [ ] Add support for bgz index 
 * - [ ] allow user to specify what is expected to be unique (ie just the ID or the genotype, or everything) 
 * - [ ] Add option to determine if vcf is sorted (probably can't be multi-threaded)
 * - [ ] Add more stats: Like N variants across N locations, counts by chromosome, presence/amount of duplicate alleles contexts, presence/amount of multiallelic sites, etc
 * @author j
 *
 */
public class VcfDetails {

    private static final Logger log = BootstrapLogger.configureLogger(VcfDetails.class.getName());
    private static final int DEFAULT_NUMBER_OF_THREADS = 2;
    private static final String DELIMETER = "\t";
    private int numberOfThreads;
    private File vcfFile;
    private CommandLine commandLine = null;
    private Duration duration;


    /**
     * Parse command line arguments and run duplicate variant finder
     *
     * @param args
     */
    public static void main(String[] args) {

        Options commandLineOptions = getCommandLineOptions();
        CommandLine commandLine=null;
        try {
            commandLine = parseCommandLine(args, commandLineOptions);
        } catch (ParseException e) {
            log.severe(e.getMessage());
            printHelp(commandLineOptions);
            System.exit(1);
        }

        // has the vcfFile argument been passed?
        if (commandLine.hasOption("help")) {
            printHelp(commandLineOptions);
            System.exit(0);
        }

        VcfDetails vcfDetails = new VcfDetails(commandLine);
        vcfDetails.start();
    }

    /**
     * Constructor
     *
     * @param commandLine
     */
    private VcfDetails(CommandLine commandLine) {
        this.commandLine = commandLine;
        verifyParameters();
    }

    /**
     * The non-static "main" method
     */
    private void start() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        log.info("Reading " + vcfFile);

        // Create a thread pool
        log.fine("Creating thread pool of size " + numberOfThreads);
        ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);

        VcfDetailsModel details = new VcfDetailsModel();

        try (VCFFileReader vcfFileReader = new VCFFileReader(vcfFile, false);
                CloseableIterator<VariantContext> iter = vcfFileReader.iterator()) {
            while (iter.hasNext()) {
                final VariantContext context = iter.next();
                VcfDetailsTask vdr = new VcfDetailsTask(context, details);
                vdr.setPrintStatusUpdates(commandLine.hasOption("showDuplicateGenotypes"));
                vdr.setPrintDuplicateGenotypes(commandLine.hasOption("showDuplicateGenotypes"));
                vdr.setPrintMultiAllelicAlternates(commandLine.hasOption("showMultiallelicAlts"));
                pool.execute(vdr);
            }
        }

        // Indicate that we are done adding tasks
        pool.shutdown();

        // Wait for the tasks to complete
        try {
            pool.awaitTermination(2, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.severe("Two hour time limit reached; shutting down.");
        }

        stopWatch.stop();
        duration = Duration.ofMillis(stopWatch.getTime());

        if (!"false".equals(commandLine.getOptionValue("summary"))) {
            printSummary(details);
        }
    }


    /**
     * Verify command line parameters
     *
     * @param args
     */
    private void verifyParameters() {

        // check if the file exists)
        vcfFile = new File(commandLine.getOptionValue("vcfFile"));
        if (!vcfFile.exists()) {
            log.severe("File not found: " + vcfFile.getName());
            System.exit(1);
        }

        // Se the number of threads
        if (commandLine.hasOption("threads")) {
            String digits = commandLine.getOptionValue("threads");
            if(!NumberUtils.isDigits(digits)) {
                log.severe("threads parameter is not numeric");
                System.exit(1);
            }

            numberOfThreads = NumberUtils.toInt(digits);
            if (numberOfThreads < 1 || numberOfThreads > 23) {
                log.severe("threads parameter must be positive value from 1 to 23");
                System.exit(1);
            }
        } else {
            numberOfThreads = DEFAULT_NUMBER_OF_THREADS;
        }
    }

    /**
     * Parse user's command line args
     *
     * @param args
     * @param options
     * @return
     * @throws ParseException
     */
    private static CommandLine parseCommandLine(String[] args, Options options) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    /**
     * Show command line arguments
     *
     * @param options
     */
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(VcfDetails.class.getSimpleName(), "Find duplicate variants in VCF", options, "");
    }

    /**
     * Return command line options
     *
     * @param args
     * @return
     * @throws ParseException
     */
    private static Options getCommandLineOptions() {
        Options options = new Options();

        options.addOption(Option.builder("f")
                .argName("vcfFile")
                .longOpt("vcfFile")
                .hasArg()
                .desc("VCF File")
                .required()
                .build());

        options.addOption(Option.builder("h")
                .argName("help")
                .longOpt("help")
                .hasArg(false)
                .desc("Help")
                .build());

        options.addOption(Option.builder("s")
                .argName("summary")
                .longOpt("summary")
                .desc("Print summary after processing is complete (default=true)")
                .build());

        options.addOption(Option.builder("d")
                .argName("showDuplicateGenotypes")
                .longOpt("showDuplicateGenotypes")
                .desc("Print all duplicate genotypes (default=false)")
                .build());

        options.addOption(Option.builder("u")
                .argName("showUpdates")
                .longOpt("showUpdates")
                .desc("Provide reassurance while processing large files (default=false)")
                .build());

        options.addOption(Option.builder("t")
                .argName("threads")
                .longOpt("threads")
                .hasArg()
                .desc("Specifies the number of threads to run (default="+DEFAULT_NUMBER_OF_THREADS+")")
                .build());

        options.addOption(Option.builder("m")
                .argName("showMultiallelicAlts")
                .longOpt("showMultiallelicAlts")
                .desc("Print all multiallelic alternates (default=false)")
                .build());

        return options;
    }

    /**
     * Print summary showing how long it took to run and how many duplicates were found.
     * @param details
     *
     * @param duration
     * @param duplicates
     */
    private void printSummary(VcfDetailsModel details) {
        log.info(String.format("Time: %dm.%ds.%dms", duration.toMinutesPart(), duration.toSecondsPart(),
                duration.toMillisPart()));
        log.info("Number of records: " + details.getNumberOfRecords());
        log.info("Number of duplicates: " + details.getNumberOfDuplicateGenotypes());
        log.info("Number of multiallelic alts: " + details.getNumberOfVariantsWithMultiAllelicAlternates());
        log.info("Chromsome-variant counts: \n" + getChromosomeVariantCounts(details));
    }

    private String getChromosomeVariantCounts(VcfDetailsModel details) {
        StringBuffer buffer = new StringBuffer();

        // Sort the chromosomes so they are always displayed in the same order
        buffer.append(String.join(DELIMETER,
                details.getChromosomeCounts().keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList())));
        buffer.append("\n");
        buffer.append(String.join(DELIMETER,
                details.getChromosomeCounts().keySet()
                .stream()
                .sorted()
                .map(x -> details.getChromosomeCounts().get(x))
                .map(String::valueOf)
                .collect(Collectors.toList())));
        return buffer.toString();
    }
}
