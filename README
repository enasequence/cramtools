CRAMTools is a set of Java tools and APIs for efficient compression of sequence read data. Although this is intended as a stable version the code is released as early access. Parts of the CRAMTools are experimental and may not be supported in the future.
http://www.ebi.ac.uk/ena/about/cram_toolkit

Version 3.0

Change log:
Switched to htsjdks as backend, which enables CRAM3 features. Please refer to http://samtools.github.io/hts-specs/CRAMv3.pdf for the CRAM file specification. 
A pre-built cramtools-3.0.jar file is included in the repository.   

Input files:
Reference sequence in fasta format <fasta file>
Reference sequence index file <fasta file>.fai created using samtools (samtools faidx <fasta file>)
Input BAM file <BAM file> sorted by reference coordinates
BAM index file <BAM file>.bai created using samtools (samtools index <BAM file>)
Download and run the program:
Download the prebuilt runnable jar file from: https://github.com/enasequence/cramtools/blob/master/cramtools-3.0.jar?raw=true
Execute the command line program: java -jar cramtools-3.0.jar
Usage is printed if no arguments were given 

To convert a BAM file to CRAM:
> java -jar cramtools-3.0.jar cram --input-bam-file <bam file> --reference-fasta-file <reference fasta file> [--output-cram-file <output cram file>]

To convert a CRAM file to BAM:
> java -jar cramtools-3.0.jar bam --input-cram-file <input cram file> --reference-fasta-file <reference fasta file> --output-bam-file <output bam file>


To build the program from source:
 
To check out the source code from github you will need git client: http://git-scm.com/
Make sure you have java 1.7 or higher: http://openjdk.java.net/ or http://www.oracle.com/us/technologies/java/index.html
Make sure you have ant version 1.7 or higher: http://ant.apache.org/
Clone the repository to your local directory:
> git clone https://github.com/enasequence/cramtools.git
Change to the directory: 
> cd cramtools
Build a runnable jar file: 
> ant -f build/build.xml runnable
Run cramtools
> java -jar cramtools-3.0.jar 


Picard integraion
Picard tools have been removed from cramtools because Picard supports CRAM via htsjdk. 


Reference sequence discovery
cramtools supports the following reference discovery mechanism: 
1. check local file provided in the command line (-R or --reference-fasta-file option)
2. download sequences from the ENA reference registry using MD5 checksums from the SAM header
3. access local cache using REF_CACHE/REF_PATH environment variables. Please refer to http://www.htslib.org/doc/samtools.html for more details.

The following tools have been included into this release: 
cram (SAM/BAM to CRAM conversion)
bam (CRAM to SAM/BAM conversion)
index (CRAM indexing, can produce CRAI or BAI index)
merge (merges several SAM/BAM/CRAM files into one)
fastq (dump reads in fastq format)
fixheader (fix CRAM file header, namely reference sequence MD5 checksums)
getref (download all reference sequences mentioned in a CRAM file)
qstat (produce some basic quality score stats for a CRAM file) 

The usage can be accessed by calling cramtools with the corresponding command as a single argument.


Lossy model
Bam2Cram allows to specify lossy model via a string which can be composed of one or more words separated by '-'.
Each word is read or base selector and quality score treatment, which can be binning (Illumina 8 bins) or full scale (40 values).
Here are some examples:
N40-D8        preserve quality scores for non-matching bases with full precision, and bin quality scores for positions flanking deletions.
m5            preserve quality scores for reads with mapping quality score lower than 5
R40X10-N40    preserve non-matching quality scores and those matching with coverage lower than 10
*8            bin all quality scores

Selectors:
R    bases matching the reference sequence
N    aligned bases mismatching the reference, this only applies to 'M', '=' (EQ) or 'X' BAM cigar elements.
U    unmapped read
Pn    pileup: capture all bases at a given position on the reference if there are at least n mismatches
D    read positions flanking a deletion
Mn    reads with mapping quality score higher than n
mn   reads with mapping quality score lower than n
I       insertions
*      all

By default no quality scores will be preserved.

Illimuna 8-binning scheme:
0, 1, 6, 6, 6, 6, 6, 6, 6, 6, 15, 15, 15, 15, 15, 15, 15, 15, 15,
15, 22, 22, 22, 22, 22, 27, 27, 27, 27, 27, 33, 33, 33, 33, 33, 37,
37, 37, 37, 37, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
40, 40, 40, 40, 40, 40 


Check for more on our web site: 
http://www.ebi.ac.uk/ena/about/cram_toolkit
