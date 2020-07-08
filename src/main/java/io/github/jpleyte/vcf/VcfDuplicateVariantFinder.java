package io.github.jpleyte.vcf;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.time.StopWatch;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import io.github.jpleyte.log.BootstrapLogger;

/**
 * Check an unsorted VCF file for duplicates.
 * Inteneded to be faster than ``grep -v "^#" your.vcf | cut -f 1,2,4,5 | sort| uniq -c | sort -rn``
 * 
 * To Do:
 * - Add support for file input via stdio
 * - Add support for bgz index
 * - Provide benchmark comparison to other methods
 * - allow user to specify what is expected to be unique - the ID or the genotype (default)
 * 
 * @author j
 *
 */
public class VcfDuplicateVariantFinder {

	private static final Logger log = BootstrapLogger.configureLogger(VcfDuplicateVariantFinder.class.getName());
	private Set<String> duplicates = null;
	private Duration duration;

	/**
	 * Parse command line arguments and run duplicate variant finder
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Options commandLineOptions = getCommandLineOptions();
		CommandLine commandLine = null;
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

		// check the filename extension
		String extension = FilenameUtils.getExtension(vcfFile.getAbsolutePath());
		boolean isCompressed = false;
		if (extension.equals("vcf")) {
			isCompressed = false;
		} else if (extension.equals("gz") || extension.equals("bgz")) {
			isCompressed = true;
		} else {
			log.severe("Unrecognised file type, expecting vcf, gz, or bgz.");
			System.exit(3);
		}

		VcfDuplicateVariantFinder vcfDuplicateVariantFinder = new VcfDuplicateVariantFinder();
		vcfDuplicateVariantFinder.processVcfFile(vcfFile, isCompressed);

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
		formatter.printHelp(VcfDuplicateVariantFinder.class.getSimpleName(), "Find duplicate variants in VCF", options,
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
				.hasArg()
				.desc("Print summary after processing is complete (default=true)")
				.build());

		options.addOption(Option.builder("d")
				.argName("showDuplicates")
				.longOpt("showDuplicates")
				.hasArg()
				.desc("Print all duplicates (default=false)")
				.build());

		return options;
	}

	/**
	 * 
	 * @param vcfFile
	 * @param isCompressed
	 * - ignored because the VCFFileReader can handle both types anyway.
	 */
	private void processVcfFile(File vcfFile, boolean isCompressed) {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		Set<String> allGenotypes = new HashSet<>();

		log.info("Reading " + vcfFile);
		try (VCFFileReader vcfFileReader = new VCFFileReader(vcfFile)) {

			// TODO make this parallel
			duplicates = vcfFileReader.iterator().stream()
					// .parallel() // Parallel causes a heap exception
					.map(this::mapToGenotype)
					.filter(n -> !allGenotypes.add(n)) // Set.add() returns false if the item was already in the set.
					.collect(Collectors.toSet());
		}

		stopWatch.stop();
		duration = Duration.ofMillis(stopWatch.getTime());
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
		log.info("Number of duplicates: " + duplicates.size());

		log.info("First duplicate: " + duplicates.stream().findFirst().orElse("n/a"));

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

		// TODO This needs to be verified and unit tests needed
		String reference = vc.getReference().getDisplayString();

		// TODO It would be preferable if you added each alt allele to the map individually rather than combining them
		String alternate = vc.getAlternateAlleles().stream()
				.map(Allele::getDisplayString)
				.sorted()
				.collect(Collectors.joining(","));

		String genotype = chromosome + "-" + position + "-" + reference + "-" + alternate;

		return genotype;
	}

}
