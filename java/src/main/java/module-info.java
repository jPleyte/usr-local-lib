/**
 *
 */
/**
 * @author j
 *
 */
module VcfDuplicateVariantFinder {
    exports io.github.jpleyte.vcf.detail;
    exports io.github.jpleyte.log;

    requires commons.cli;
    requires htsjdk;
    requires java.logging;
    requires org.apache.commons.io;
    requires org.apache.commons.lang3;
}