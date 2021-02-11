package io.github.jpleyte.vcf.detail;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Data from each VcfDetails thread is collected in this model object.
 * 
 * @author j
 *
 */
public class VcfDetailsModel {
    public static final String[] CHROMOSOMES = { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
            "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "MT", "X", "Y" };

    private AtomicInteger numberOfRecords = new AtomicInteger();
    private AtomicInteger numberOfDuplicateGenotypes = new AtomicInteger();
    private AtomicInteger numberOfVariantsWithMultiAllelicAlternates = new AtomicInteger();
    private Map<String, AtomicInteger> chromosomeCounts = new ConcurrentHashMap<>();

    public VcfDetailsModel() {
        for (String chromosome : CHROMOSOMES) {
            chromosomeCounts.put(chromosome, new AtomicInteger());
        }
    }

    public AtomicInteger getNumberOfRecords() {
        return numberOfRecords;
    }

    public AtomicInteger getNumberOfDuplicateGenotypes() {
        return numberOfDuplicateGenotypes;
    }

    public AtomicInteger getNumberOfVariantsWithMultiAllelicAlternates() {
        return numberOfVariantsWithMultiAllelicAlternates;
    }

    public Map<String, Integer> getChromosomeCounts() {
        return Collections.unmodifiableMap(chromosomeCounts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, y -> y.getValue().get())));
    }
    /**
     * Increment the number of variants found on this chromosome. Returns true if
     * this is the first variant on the chromsome.
     * 
     * @param chromosome
     * @return
     */
    public boolean incrementChromosomeVariantCount(String chromosome) {
        chromosomeCounts.computeIfAbsent(chromosome, x -> new AtomicInteger());
        if (chromosomeCounts.get(chromosome).getAndIncrement() == 0) {
            return true;
        } else {
            return false;
        }
    }

}
