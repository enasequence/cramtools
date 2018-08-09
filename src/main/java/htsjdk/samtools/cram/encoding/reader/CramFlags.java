/*
 * Copyright 2012 - 2018 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
