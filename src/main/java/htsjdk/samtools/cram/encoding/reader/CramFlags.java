package htsjdk.samtools.cram.encoding.reader;

/**
 * Created by vadim on 28/07/2015.
 */
class CramFlags {
     static final int MULTI_FRAGMENT_FLAG = 0x1;
     static final int PROPER_PAIR_FLAG = 0x2;
     static final int SEGMENT_UNMAPPED_FLAG = 0x4;
     static final int NEGATIVE_STRAND_FLAG = 0x10;
     static final int FIRST_SEGMENT_FLAG = 0x40;
     static final int LAST_SEGMENT_FLAG = 0x80;
     static final int SECONDARY_ALIGNMENT_FLAG = 0x100;
     static final int VENDOR_FILTERED_FLAG = 0x200;
     static final int DUPLICATE_FLAG = 0x400;
     static final int SUPPLEMENTARY_FLAG = 0x800;

     static final int MATE_NEG_STRAND_FLAG = 0x1;
     static final int MATE_UNMAPPED_FLAG = 0x2;

     static final int FORCE_PRESERVE_QS_FLAG = 0x1;
     static final int DETACHED_FLAG = 0x2;
     static final int HAS_MATE_DOWNSTREAM_FLAG = 0x4;
     static final int UNKNOWN_BASES = 0x8;
}
