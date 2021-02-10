package io.github.jpleyte.vcf.detail;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import io.github.jpleyte.log.BootstrapLogger;

/**
 * This is the worker thread for the VcfDetails app
 * To Do:
 *  - Try parallel processing the chromosome stream
 *  -
 */
public class VcfDetailsTask implements Runnable {
    private static final Logger log = BootstrapLogger.configureLogger(VcfDetailsTask.class.getName());

    private static final int STATUS_UPDATE_FREQUENCY = 500000000;

    private final Set<String> genotypes = new HashSet<>();

    final VcfDetailsModel vcfDetailsModel;
    final VariantContext variantContext;

    boolean printStatusUpdates = false;
    boolean printDuplicateGenotypes = false;
    boolean printMultiAllelicAlternates;

    /**
     * Constructor
     * 
     * @param variantContext
     * @param details
     */
    public VcfDetailsTask(VariantContext variantContext, VcfDetailsModel details) {
        this.vcfDetailsModel = details;
        this.variantContext = variantContext;
    }

    int lastPosition = 0;

    @Override
    public void run() {
        analyse(variantContext);
    }

    /**
     * Run each analysis function on the current VariantContext
     *
     * @param vc
     */
    private void analyse(VariantContext vc) {
        if (vcfDetailsModel.incrementChromosomeVariantCount(vc.getContig())) {
            log.info("Starting chromosome " + vc.getContig());
        }

        countRecords();

        if (printStatusUpdates) {
            printStatusUpdate(vc);
        }

        checkForDuplicateGenotype(vc);

        checkForMultiAllelicAlternate(vc);
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

    /**
     * Keep track of how many records are in the VCF
     */
    private void countRecords() {
        vcfDetailsModel.getNumberOfRecords().incrementAndGet();
    }

    /**
     * Print a status message after processing every nth record
     *
     * @param vc
     */
    private void printStatusUpdate(VariantContext vc) {
        int recordsProcessed = vcfDetailsModel.getNumberOfRecords().get();
        if (recordsProcessed % STATUS_UPDATE_FREQUENCY == 0 && recordsProcessed > 0) {
            log.info("Processing record " + recordsProcessed + ", contig=" + vc.getContig());
        }
    }

    /**
     * Determine if the variant has already been encountered, and print its genotype
     * if it is.
     *
     * @param vc
     */
    private void checkForDuplicateGenotype(VariantContext vc) {
        String genotype = mapToGenotype(vc);
        if (!genotypes.add(genotype)) {
            vcfDetailsModel.getNumberOfDuplicateGenotypes().incrementAndGet();

            if (printDuplicateGenotypes) {
                log.info("Duplicate: " + genotype);
            }
        }
    }

    /**
     * The VCF spec allows the alt allele to have more than one value.
     *
     * @param vc
     */
    private void checkForMultiAllelicAlternate(VariantContext vc) {
        if (vc.getAlternateAlleles().size() > 1) {
            vcfDetailsModel.getNumberOfVariantsWithMultiAllelicAlternates().incrementAndGet();
            if (printMultiAllelicAlternates) {
                log.info("Multiallelic alt at " + mapToGenotype(vc));
            }
        }
    }

    public boolean isPrintStatusUpdates() {
        return printStatusUpdates;
    }

    public void setPrintStatusUpdates(boolean printStatusUpdates) {
        this.printStatusUpdates = printStatusUpdates;
    }

    public boolean isPrintDuplicateGenotypes() {
        return printDuplicateGenotypes;
    }

    public void setPrintDuplicateGenotypes(boolean printDuplicateGenotypes) {
        this.printDuplicateGenotypes = printDuplicateGenotypes;
    }

    public boolean isPrintMultiAllelicAlternates() {
        return printMultiAllelicAlternates;
    }

    public void setPrintMultiAllelicAlternates(boolean printMultiAllelicAlternates) {
        this.printMultiAllelicAlternates = printMultiAllelicAlternates;
    }

}