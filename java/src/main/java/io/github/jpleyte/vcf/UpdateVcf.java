package io.github.jpleyte.vcf;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import io.github.jpleyte.log.BootstrapLogger;

/**
 * Example of how to add INFO fields to a VCF using HTSJdk
 * @author pleyte
 *
 */
public class UpdateVcf {
    private static final Logger log = BootstrapLogger.configureLogger(UpdateVcf.class.getName());

    private static final String INFO_FIELD_FOO = "FOO";
    private static final String INFO_FIELD_BAR = "BAR";
    
    private File inVcfFile;
    private File outVcfFile;

    public static void main(String[] args) throws IOException {
        UpdateVcf updateVcf = new UpdateVcf("/tmp/inVcf.vcf", "/tmp/outVcf.vcf");
        updateVcf.update();
    }

    public UpdateVcf(String inFile, String outFile) {
        inVcfFile = new File(inFile);
        outVcfFile = new File(outFile);

        if (!inVcfFile.exists()) {
            log.severe("Input file does not exist: " + inVcfFile);
            System.exit(-1);
        }
    }

    /**
     *
     * @throws IOException
     */
    private void update() throws IOException {
        long n = 0;
        try(VCFFileReader vcfReader = new VCFFileReader(inVcfFile, false);
                CloseableIterator<VariantContext> iter = vcfReader.iterator()) {
            VCFHeader header = addFieldDefinitionsToHeader(vcfReader.getFileHeader());
            SAMSequenceDictionary dict = header.getSequenceDictionary();

            try(VariantContextWriter writer = new VariantContextWriterBuilder()
                    .setOutputPath(outVcfFile.toPath())
                    .setReferenceDictionary(dict)
                    .modifyOption(Options.INDEX_ON_THE_FLY, false)
                    .build()) {
                writer.writeHeader(header);
                while(iter.hasNext()) {                    
                    VariantContext vc =  new VariantContextBuilder(iter.next())
                            .attribute(INFO_FIELD_FOO, "foo_"+String.valueOf(n++))
                            .attribute(INFO_FIELD_BAR, "bar_"+String.valueOf(n++))
                            .make();
                    writer.add(vc);
                }
            }
        }
    }

    /**
     * Add the INFO field definitions to a VCF header
     * @param header
     * @return
     */
    private VCFHeader addFieldDefinitionsToHeader(VCFHeader header) {
        header.addMetaDataLine(new VCFInfoHeaderLine(INFO_FIELD_FOO, 1, VCFHeaderLineType.String, "A foo value"));        
        header.addMetaDataLine(new VCFInfoHeaderLine(INFO_FIELD_BAR, 1, VCFHeaderLineType.String, "A bar value"));
        return header;
    }
}
