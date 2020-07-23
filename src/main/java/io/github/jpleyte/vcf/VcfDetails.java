package io.github.jpleyte.vcf;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import io.github.jpleyte.log.BootstrapLogger;

/**
 * Analyse a VCF by running a set of functions on each line. 
 * 
 * To Do:
 * - Try using HTSLib instead of HtsJdk
 * - Use multiple threads  
 * - Add support for file input via stdio
 * - Add support for bgz index
 * - allow user to specify what is expected to be unique (ie just the ID or the genotype, or everything)
 * 
 * @author j
 *
 */
public class VcfDetails {

	private static final Logger log = BootstrapLogger.configureLogger(VcfDetails.class.getName());
	private static final int STATUS_UPDATE_FREQUENCY = 500000000;
	private CommandLine commandLine = null;
	private Duration duration;
	private int records=0; 

	private Set<String> duplicates = new HashSet<>();
	private final Set<String> allGenotypes = new HashSet<>();
	
	private final Map<String, String> multiAllelicAlternates = new HashMap<>();
	
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

		File vcfFile = new File(commandLine.getOptionValue("vcfFile"));

		// check if the file exists
		if (!vcfFile.exists()) {
			log.severe("File not found: " + vcfFile.getName());
			System.exit(2);
		}

		VcfDetails vcfDuplicateVariantFinder = new VcfDetails();
		vcfDuplicateVariantFinder.processVcfFile(vcfFile, commandLine);

		if (!"false".equals(commandLine.getOptionValue("s"))) {
			vcfDuplicateVariantFinder.printSummary();
		}
		if ("true".equals(commandLine.getOptionValue("d"))) {
			vcfDuplicateVariantFinder.printDuplicates();
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
		formatter.printHelp(VcfDetails.class.getSimpleName(), "Find duplicate variants in VCF", options,
				"");

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
				.argName("showDuplicates")
				.longOpt("showDuplicates")
				.desc("Print all duplicates (default=false)")
				.build());
		
		options.addOption(Option.builder("u")
				.argName("showUpdates")
				.longOpt("showUpdates")
				.desc("Provide reassurance while processing large files (default=false)")
				.build());

		return options;
	}

	/**
	 * 
	 * @param vcfFile
	 * @param isCompressed
	 * - ignored because the VCFFileReader can handle both types anyway.
	 */
	private void processVcfFile(File vcfFile, CommandLine commandLine) {
		this.commandLine = commandLine;
		
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		log.info("Reading " + vcfFile);
		try (VCFFileReader vcfFileReader = new VCFFileReader(vcfFile)) {
			vcfFileReader.iterator()
				.stream()
				.forEach(this::analyse);
		}

		stopWatch.stop();
		duration = Duration.ofMillis(stopWatch.getTime());
	}

	/**
	 * Run each analysis function on the current VariantContext
	 * @param vc
	 */
	private void analyse(VariantContext vc) {
		countRecords();
		
		if(!"false".equals(commandLine.getOptionValue("updates"))) {
			printStatusUpdate(vc);	
		}
		
		checkForDuplicate(vc);
	
		checkForMultiAllelicAlternate(vc);
	}
	
	/**
	 * 
	 * @param vc
	 */
	private void checkForMultiAllelicAlternate(VariantContext vc) {
		if(vc.getAlternateAlleles().size() > 1) {
			multiAllelicAlternates.put(mapToGenotype(vc), 
					vc.getAlternateAlleles().stream()
						.map(Allele::getBaseString)
						.collect(Collectors.joining(", ")));
		}
		
	}
	
	/**
	 * Print a status message after processing every nth record
	 * @param vc
	 */
	private void printStatusUpdate(VariantContext vc) {
		if(records % STATUS_UPDATE_FREQUENCY == 0 && records > 0) {
			log.info("Processing record " + records+", contig="+vc.getContig());
		}
	}

	private void checkForDuplicate(VariantContext vc) {
		String genotype = mapToGenotype(vc);
		if(!allGenotypes.add(genotype)) {
			duplicates.add(genotype);
		}
	}

	private void countRecords() {
		records = records + 1;
	}

	/**
	 * Print summary showing how long it took to run and how many duplicates were found.
	 * 
	 * @param duration
	 * @param duplicates
	 */
	private void printSummary() {
		log.info(String.format("Time: %dm.%ds.%dms", duration.toMinutesPart(), duration.toSecondsPart(),
				duration.toMillisPart()));
		log.info("Number of records: " + records);
		log.info("Number of duplicates: " + duplicates.size());
		log.info("Number of multiallelic alts: " + multiAllelicAlternates.size());
		
		if(!duplicates.isEmpty()) {
			log.info("First duplicate: " + duplicates.stream().findFirst().orElse("n/a"));	
		}
		
		if(!multiAllelicAlternates.isEmpty()) {
			Map.Entry<String,String> alt = multiAllelicAlternates.entrySet().stream().findFirst().orElse(null);
			log.info("First multiallelic alt: " + alt.getKey()+" " + alt.getValue());
		}
	}

	/**
	 * Print all the duplicate genotypes found
	 */
	private void printDuplicates() {
		log.info("Duplicates: ");
		duplicates.stream().forEach(log::info);
	}
	
	/**
	 * Return a string representation of the variant
	 * 
	 * @param vc
	 * @return
	 */
	private String mapToGenotype(VariantContext vc) {
		String chromosome = vc.getContig();
		int position = vc.getStart();

		String reference = vc.getReference().getDisplayString();

		String alternate = vc.getAlternateAlleles().stream()
				.map(Allele::getDisplayString)
				.sorted()
				.collect(Collectors.joining(","));

		String genotype = chromosome + "-" + position + "-" + reference + "-" + alternate;

		return genotype;
	}

}
