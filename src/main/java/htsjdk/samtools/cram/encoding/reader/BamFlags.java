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
public class BamFlags {
    public static final int READ_PAIRED_FLAG = 0x1;
    public static final int PROPER_PAIR_FLAG = 0x2;
    public static final int READ_UNMAPPED_FLAG = 0x4;
    public static final int MATE_UNMAPPED_FLAG = 0x8;
    public static final int READ_STRAND_FLAG = 0x10;
    public static final int MATE_STRAND_FLAG = 0x20;
    public static final int FIRST_OF_PAIR_FLAG = 0x40;
    public static final int SECOND_OF_PAIR_FLAG = 0x80;
    public static final int NOT_PRIMARY_ALIGNMENT_FLAG = 0x100;
    public static final int READ_FAILS_VENDOR_QUALITY_CHECK_FLAG = 0x200;
    public static final int DUPLICATE_READ_FLAG = 0x400;
    public static final int SUPPLEMENTARY_FLAG = 0x800;
}
