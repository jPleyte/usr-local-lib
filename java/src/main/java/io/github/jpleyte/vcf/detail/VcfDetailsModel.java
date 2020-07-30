package io.github.jpleyte.vcf.detail;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Data from each VcfDetails thread is collected in this model object.
 * 
 * @author j
 *
 */
public class VcfDetailsModel {
    private AtomicInteger numberOfRecords = new AtomicInteger();
    private AtomicInteger numberOfDuplicates = new AtomicInteger();
    private AtomicInteger numberOfVariantsWithMultiAllelicAlternates = new AtomicInteger();

    public AtomicInteger getNumberOfRecords() {
        return numberOfRecords;
    }

    public AtomicInteger getNumberOfDuplicates() {
        return numberOfDuplicates;
    }

    public AtomicInteger getNumberOfVariantsWithMultiAllelicAlternates() {
        return numberOfVariantsWithMultiAllelicAlternates;
    }

}
