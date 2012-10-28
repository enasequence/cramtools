package net.sf.cram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.swing.tree.DefaultMutableTreeNode;

import net.sf.block.Block;
import net.sf.block.CompressionMethod;
import net.sf.block.Container;
import net.sf.block.Definition;
import net.sf.block.Format;
import net.sf.block.FormatFactory;
import net.sf.picard.reference.ReferenceSequenceFile;
import net.sf.picard.reference.ReferenceSequenceFileFactory;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTagUtil;
import uk.ac.ebi.ena.sra.cram.SequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.Utils;
import uk.ac.ebi.ena.sra.cram.bam.Sam2CramRecordFactory;
import uk.ac.ebi.ena.sra.cram.encoding.MeasuringCodec;
import uk.ac.ebi.ena.sra.cram.format.CramFormatException;
import uk.ac.ebi.ena.sra.cram.format.CramHeader;
import uk.ac.ebi.ena.sra.cram.format.CramReadGroup;
import uk.ac.ebi.ena.sra.cram.format.CramRecord;
import uk.ac.ebi.ena.sra.cram.format.CramRecordBlock;
import uk.ac.ebi.ena.sra.cram.format.CramReferenceSequence;
import uk.ac.ebi.ena.sra.cram.format.ReadTag;
import uk.ac.ebi.ena.sra.cram.format.compression.CramCompressionException;
import uk.ac.ebi.ena.sra.cram.impl.ByteArraySequenceBaseProvider;
import uk.ac.ebi.ena.sra.cram.impl.CramHeaderIO;
import uk.ac.ebi.ena.sra.cram.impl.CramRecordBlockReader;
import uk.ac.ebi.ena.sra.cram.impl.CramRecordBlockWriter;
import uk.ac.ebi.ena.sra.cram.impl.ReadFeatures2Cigar;
import uk.ac.ebi.ena.sra.cram.impl.RecordCodecFactory;
import uk.ac.ebi.ena.sra.cram.impl.RestoreBases;
import uk.ac.ebi.ena.sra.cram.impl.RestoreQualityScores;
import uk.ac.ebi.ena.sra.cram.io.BitInputStream;
import uk.ac.ebi.ena.sra.cram.io.BitOutputStream;
import uk.ac.ebi.ena.sra.cram.io.DefaultBitInputStream;
import uk.ac.ebi.ena.sra.cram.io.DefaultBitOutputStream;
import uk.ac.ebi.ena.sra.cram.spot.PairedTemplateAssembler;
import uk.ac.ebi.ena.sra.cram.stats.CramStats;

public class Coder {
	private int recordsPerSlice;

	public Coder(int recordsPerSlice) {
		this.recordsPerSlice = recordsPerSlice;
	}

	public static void main(String[] args) throws IOException, CramCompressionException, CramFormatException {
		File bamFile;
		File refFile;
		String seqName;
		int maxRecords;
		int recordsPerBlock;
		int recordsPerSlice;

		if (args.length == 0) {
			Properties props = new Properties();
			props.load(new FileInputStream("options"));
			bamFile = new File(props.getProperty("bamFile"));
			refFile = new File(props.getProperty("refFile"));
			seqName = props.getProperty("seqName");
			maxRecords = Integer.valueOf(props.getProperty("maxRecords"));
			recordsPerBlock = Integer.valueOf(props.getProperty("recordsPerBlock"));
			recordsPerSlice = Integer.valueOf(props.getProperty("recordsPerSlice"));
		} else {
			bamFile = new File(args[0]);
			refFile = new File(args[1]);
			seqName = args[2];
			maxRecords = 100000;
			recordsPerBlock = 100000;
			recordsPerSlice = 1000;
		}
		
		System.out.println(SAMTagUtil.getSingleton().makeBinaryTag("OQ"));

		ReferenceSequenceFile refSeqFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(refFile);

		byte[] refBytes = refSeqFile.getSequence(seqName).getBases();
		Sam2CramRecordFactory scFactory = new Sam2CramRecordFactory(refBytes);
		scFactory.captureAllTags = true;
		scFactory.captureUnmappedBases = true;
		scFactory.captureUnmappedScores = true;
		scFactory.losslessQS = true;
		scFactory.preserveReadNames = true;
		SequenceBaseProvider provider = new ByteArraySequenceBaseProvider(refBytes);

		SAMFileReader reader = new SAMFileReader(bamFile);
		SAMRecordIterator iterator = reader.query(seqName, 0, 0, false);
		List<CramRecord> records = new ArrayList<CramRecord>();

		PairedTemplateAssembler a = new PairedTemplateAssembler();
		int counter = 0;
		int bases = 0;
		long size = 0;
		
		Definition definition = new Definition();
		definition.contentId = "input.bam".getBytes();
		definition.formatMajor = 1;
		definition.formatMinor = 0;
		definition.magick = "CRAM".getBytes();
		Format format = new FormatFactory().createFormat(definition);

		SAMRecord samRecord = null;
		CramHeader cramHeader = createCramHeader(reader.getFileHeader());
		ByteArrayOutputStream hBaos= new ByteArrayOutputStream() ;
		Container hC = new Container() ;
		Block hBlock = new Block() ;
		hBlock.contentId=0;
		hBlock.contentType=0;
		hBlock.method = CompressionMethod.GZIP.byteValue() ;
		hC.blocks = new Block[]{hBlock} ;
		hC.containers= new Container[0] ;
		CramHeaderIO.write(cramHeader, hBaos) ;
		hBlock.data = hBaos.toByteArray() ;
		format.writeContainer(hC, hBaos) ;
		System.out.println("Header size: " + hBaos.size());
		
		for (int b = 0; b < maxRecords; b+=recordsPerBlock) {
			bases = 0 ;
			size = 0 ;
			for (int i = 0; i < recordsPerBlock; i++) {
				samRecord = iterator.next();
				a.addSAMRecord(samRecord);
				bases += samRecord.getReadLength();
			}

			for (int i = 0; i < recordsPerBlock; i++) {
				while ((samRecord = a.nextSAMRecord()) != null) {
					CramRecord cramRecord = scFactory.createCramRecord(samRecord);
					records.add(cramRecord);

					setPairingInformation(cramRecord, samRecord, a.distanceToNextFragment());
				}
				while ((samRecord = a.fetchNextSAMRecord()) != null) {
					CramRecord cramRecord = scFactory.createCramRecord(samRecord);
					records.add(cramRecord);

					setPairingInformation(cramRecord, samRecord, a.distanceToNextFragment());
				}

			}

			Container container = new Coder(recordsPerSlice).writeRecords(cramHeader, records, provider);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			long time1 = System.nanoTime();
			format.writeContainer(container, baos);
			long time2 = System.nanoTime();
			System.out.printf("Container written: bytes=%d in %.2f ms.\n", baos.size(), (time2 - time1) / 1000000f);
			size += baos.size();

			records.clear();

			System.out.printf("Total records: %d, bases=%d, bytes=%d, b/b=%.2f\n", counter, bases, size, 8f * size
					/ bases);

			time1 = System.nanoTime();
			Container c = format.readContainer(new ByteArrayInputStream(baos.toByteArray()));
			time2 = System.nanoTime();
			System.out.printf("Container read in %.2f ms.\n", (time2 - time1) / 1000000f);

			time1 = System.nanoTime();
			List<CramRecord> readRecords = new Coder(recordsPerSlice).readRecords(c, cramHeader, provider);
			time2 = System.nanoTime();
			System.out.printf("Cram records read in in %.2f ms.\n", (time2 - time1) / 1000000f);
			System.out.println(readRecords.size());

			RestoreBases rb = new RestoreBases(provider, "20");
			time1 = System.nanoTime();
			for (CramRecord r : readRecords)
				rb.restoreReadBases(r);
			time2 = System.nanoTime();
			System.out.printf("Bases restored in in %.2f ms.\n", (time2 - time1) / 1000000f);

			RestoreQualityScores rq = new RestoreQualityScores();
			time1 = System.nanoTime();
			for (CramRecord r : readRecords)
				rq.restoreQualityScores(r);
			time2 = System.nanoTime();
			System.out.printf("Scores restored in in %.2f ms.\n", (time2 - time1) / 1000000f);

			time1 = System.nanoTime();
			restoreNamesAndSpots("name", 1, readRecords);
			time2 = System.nanoTime();
			System.out.printf("Pairing restored in in %.2f ms.\n", (time2 - time1) / 1000000f);
			// for (int i = 0; i < 10; i++)
			// System.out.println(readRecords.get(i));
			// for (int i = readRecords.size() - 10; i < readRecords.size();
			// i++)
			// System.out.println(readRecords.get(i));

			time1 = System.nanoTime();
			SAMFileHeader header = Utils.cramHeader2SamHeader(cramHeader);
			List<SAMRecord> samRecords = convert(readRecords, header, null, seqName);
			for (SAMRecord r : samRecords)
				Utils.calculateMdAndNmTags(r, refBytes, true, true);
			time2 = System.nanoTime();
			System.out.printf("SAMRecords restored in in %.2f ms.\n", (time2 - time1) / 1000000f);

			// for (int i = 0; i < 10; i++)
			// System.out.print(samRecords.get(i).getSAMString());
			// for (int i = samRecords.size() - 10; i < samRecords.size(); i++)
			// System.out.print(samRecords.get(i).getSAMString());

		}

		reader.close();
	}

	private static List<SAMRecord> convert(List<CramRecord> readRecords, SAMFileHeader header,
			Map<Integer, String> readGroups, String seqName) {
		Map<String, Integer> seqNameToIndexMap = new HashMap<String, Integer>();
		for (SAMSequenceRecord seq : header.getSequenceDictionary().getSequences()) {
			seqNameToIndexMap.put(seq.getSequenceName(), seq.getSequenceIndex());
		}

		ReadFeatures2Cigar readFeatures2Cigar = new ReadFeatures2Cigar();
		List<SAMRecord> samRecords = new ArrayList<SAMRecord>();
		SAMRecord samRecord = null;
		int counter = 0;
		for (CramRecord record : readRecords) {
			CramRecord cramRecord = record;
			samRecord = new SAMRecord(header);
			if (record.tags != null && !record.tags.isEmpty()) {
				for (ReadTag rt : record.tags) {
					samRecord.setAttribute(rt.getKey(), rt.getValue());
				}
			}

			if (readGroups != null && !readGroups.isEmpty()) {
				String rgId = readGroups.get(cramRecord.getReadGroupID());
				if (rgId != null)
					samRecord.setAttribute("RG", rgId);
			}

			if (cramRecord.next != null || cramRecord.previous != null) {
				CramRecord mate = cramRecord.next == null ? cramRecord.previous : cramRecord.next;
				samRecord.setReadPairedFlag(true);
				samRecord.setMateAlignmentStart((int) mate.getAlignmentStart());
				samRecord.setMateNegativeStrandFlag(mate.isNegativeStrand());
				samRecord.setInferredInsertSize(record.insertSize);

				if (SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(mate.getSequenceName())) {
					samRecord.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
					samRecord.setMateUnmappedFlag(!mate.isReadMapped());
					if (cramRecord.isFirstInPair()) {
						samRecord.setFirstOfPairFlag(true);
						samRecord.setSecondOfPairFlag(false);
					} else {
						samRecord.setFirstOfPairFlag(false);
						samRecord.setSecondOfPairFlag(true);
					}
				} else {
					if (mate.getSequenceName() == null || !seqNameToIndexMap.containsKey(mate.getSequenceName())) {
						samRecord.setReadPairedFlag(false);
						samRecord.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);
						samRecord.setMateNegativeStrandFlag(false);
					} else {
						int seqIndex = seqNameToIndexMap.get(mate.getSequenceName());
						samRecord.setMateReferenceIndex(seqIndex);
						samRecord.setMateUnmappedFlag(!mate.isReadMapped());
						if (cramRecord.isFirstInPair()) {
							samRecord.setFirstOfPairFlag(true);
							samRecord.setSecondOfPairFlag(false);
						} else {
							samRecord.setFirstOfPairFlag(false);
							samRecord.setSecondOfPairFlag(true);
						}
					}
				}
				samRecord.setReadName(cramRecord.getReadName());
			} else {
				String readName = cramRecord.getReadName();
				if (readName == null)
					readName = String.valueOf(counter);

				if (!cramRecord.isLastFragment()) {
					samRecord.setFirstOfPairFlag(cramRecord.isFirstInPair());
					samRecord.setSecondOfPairFlag(!cramRecord.isFirstInPair());
					samRecord.setReadPairedFlag(true);
				} else {
					if (cramRecord.isLastFragment()) {
						samRecord.setReadName(readName);
						samRecord.setReadPairedFlag(false);
						samRecord.setFirstOfPairFlag(false);
						samRecord.setSecondOfPairFlag(false);
					} else {
						samRecord.setReadName(readName);
						samRecord.setFirstOfPairFlag(cramRecord.isFirstInPair());
						samRecord.setSecondOfPairFlag(!cramRecord.isFirstInPair());
						samRecord.setReadPairedFlag(true);
					}
				}
			}

			samRecord.setMappingQuality((int) cramRecord.getMappingQuality() & 0xFF);
			if (cramRecord.isReadMapped()) {
				samRecord.setAlignmentStart((int) cramRecord.getAlignmentStart());
				samRecord.setReadBases(cramRecord.getReadBases());
				byte[] scores = cramRecord.getQualityScores();
				scores = cramRecord.getQualityScores();

				injectQualityScores(scores, samRecord);
			} else {
				samRecord.setAlignmentStart((int) cramRecord.getAlignmentStart());
				samRecord.setReadBases(cramRecord.getReadBases());
				byte[] scores = cramRecord.getQualityScores();
				injectQualityScores(scores, samRecord);
			}
			samRecord.setCigar(readFeatures2Cigar.getCigar2(cramRecord.getReadFeatures(),
					(int) cramRecord.getReadLength()));
			samRecord.setReadUnmappedFlag(!cramRecord.isReadMapped());
			samRecord.setReadNegativeStrandFlag(cramRecord.isNegativeStrand());
			samRecord.setReferenceName(seqName);
			samRecord.setProperPairFlag(cramRecord.isProperPair());
			samRecord.setDuplicateReadFlag(cramRecord.isDuplicate());
			samRecord.setReadFailsVendorQualityCheckFlag(cramRecord.vendorFiltered);

			if (SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(samRecord.getReferenceName()))
				samRecord.setAlignmentStart(SAMRecord.NO_ALIGNMENT_START);

			if (samRecord.getReadUnmappedFlag()) {
				samRecord.setMappingQuality(SAMRecord.NO_MAPPING_QUALITY);
				samRecord.setCigarString(SAMRecord.NO_ALIGNMENT_CIGAR);
			}
			samRecords.add(samRecord);
		}
		return samRecords;
	}

	private static int defaultQS = 63;

	private static final void injectQualityScores(byte[] scores, SAMRecord record) {
		if (scores == null || scores.length == 0) {
			injectNullQualityScores(record);
			return;
		}
		final byte nullQS = -1;
		final byte asciiOffset = 33;
		final byte space = 32;

		boolean nonDefaultQsFound = false;
		for (int i = 0; i < scores.length; i++)
			if (scores[i] != space) {
				nonDefaultQsFound = true;
				break;
			}

		if (!nonDefaultQsFound) {
			injectNullQualityScores(record);
			return;
		}

		for (int i = 0; i < scores.length; i++) {
			scores[i] -= asciiOffset;
			if (scores[i] == nullQS)
				scores[i] = (byte) (defaultQS - asciiOffset);
		}

		record.setBaseQualities(scores);
	}

	private static final void injectNullQualityScores(SAMRecord record) {
		record.setBaseQualities(SAMRecord.NULL_QUALS);
	}

	private static void restoreNamesAndSpots(String prefix, int counter, List<CramRecord> records) {
		for (int i = 0; i < records.size(); i++) {
			CramRecord cramRecord = records.get(i);
			if (cramRecord.getReadName() == null) {
				String name = prefix + (counter++);
				cramRecord.setReadName(name);
				if (!cramRecord.isLastFragment()) {
					long d = cramRecord.getRecordsToNextFragment();
					CramRecord next = records.get((int) (i + d));
					next.setReadName(name);
					cramRecord.next = next;
					next.previous = cramRecord;
				}
			}
		}
	}

	private static CramHeader createCramHeader(SAMFileHeader samHeader) {
		CramHeader header = new CramHeader();
		header.setRecords(Utils.getCramHeaderRecords(samHeader));

		List<CramReferenceSequence> sequences = new ArrayList<CramReferenceSequence>();

		for (SAMSequenceRecord seq : samHeader.getSequenceDictionary().getSequences()) {
			CramReferenceSequence cramSeq = new CramReferenceSequence(seq.getSequenceName(), seq.getSequenceLength());
			sequences.add(cramSeq);
		}

		sequences.add(new CramReferenceSequence(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME, 0));

		List<CramReadGroup> cramReadGroups = new ArrayList<CramReadGroup>(samHeader.getReadGroups().size() + 1);
		cramReadGroups.add(new CramReadGroup(null));
		Map<String, Integer> readGroupIdToIndexMap = new TreeMap<String, Integer>();
		for (SAMReadGroupRecord rgr : samHeader.getReadGroups()) {
			readGroupIdToIndexMap.put(rgr.getReadGroupId(), readGroupIdToIndexMap.size() + 1);
			cramReadGroups.add(new CramReadGroup(rgr.getReadGroupId(), rgr.getSample()));
		}

		header.setReferenceSequences(sequences);
		header.setReadGroups(cramReadGroups);
		return header;
	}

	private static void setPairingInformation(CramRecord cramRecord, SAMRecord record, long distanceToNextFragment) {
		long beyondHorizon = 0, extraChromosomePairs = 0;
		if (record.getReadPairedFlag()) {
			if (distanceToNextFragment > 0) {
				cramRecord.setLastFragment(false);
				cramRecord.setRecordsToNextFragment(distanceToNextFragment);
			} else {
				if (distanceToNextFragment == PairedTemplateAssembler.POINTEE_DISTANCE_NOT_SET)
					cramRecord.setLastFragment(true);
				else {
					cramRecord.detached = true;
					cramRecord.setLastFragment(false);
					if (distanceToNextFragment == PairedTemplateAssembler.DISTANCE_NOT_SET) {
						if (record.getReferenceName().equals(record.getMateReferenceName())) {
							beyondHorizon++;
						} else
							extraChromosomePairs++;

						cramRecord.setReadName(record.getReadName());
						cramRecord.setSequenceName(record.getReferenceName());
						CramRecord mate = new CramRecord();
						mate.setAlignmentStart(record.getMateAlignmentStart());
						mate.setNegativeStrand(record.getMateNegativeStrandFlag());
						mate.setSequenceName(record.getMateReferenceName());
						mate.setReadName(record.getReadName());
						mate.setReadMapped(!record.getMateUnmappedFlag());

						mate.detached = true;
						if (record.getFirstOfPairFlag()) {
							cramRecord.next = mate;
							mate.previous = cramRecord;
						} else {
							cramRecord.previous = mate;
							mate.next = cramRecord;
						}
					} else
						throw new RuntimeException("Unknown paired distance code: " + distanceToNextFragment);
				}
			}
		} else
			cramRecord.setLastFragment(true);
	}

	public Container writeRecords(CramHeader cramHeader, Collection<CramRecord> records, SequenceBaseProvider provider)
			throws IOException, CramCompressionException {
		List<Block> blocks = new ArrayList<Block>();

		CramStats stats = new CramStats(cramHeader, null);
		for (CramRecord record : records)
			stats.addRecord(record);

		CramRecordBlock crBlock = new CramRecordBlock();
		stats.adjustBlock(crBlock);
		crBlock.setSequenceName("20");
		crBlock.setRecordCount(records.size());

		ByteArrayOutputStream bhBAOS = new ByteArrayOutputStream();
		CramRecordBlockWriter w = new CramRecordBlockWriter(bhBAOS);
		w.write(crBlock);
		Block block = new Block();
		block.contentType = ContentType.HEADER.getContentType();
		block.contentId = 0;
		block.data = bhBAOS.toByteArray();
		block.method = CompressionMethod.GZIP.byteValue();
		blocks.add(block);

		CramRecordCodec codec = buildCodec(cramHeader, crBlock, provider);

		List<Short> tagCodes = new ArrayList<Short>();
		tagCodes.add(SAMTagUtil.getSingleton().makeBinaryTag("OQ"));

		HashMap<Integer, MyDataOutputStream> osMap = new HashMap<Integer, MyDataOutputStream>();
		osMap.put((int)ContentType.QUAL.getContentType(), new MyDataOutputStream());
		for (short code : tagCodes)
			osMap.put((int) code, new MyDataOutputStream());

		codec.osMap = osMap;

		int counter = 0;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BitOutputStream bos = new DefaultBitOutputStream(baos);

		Iterator<CramRecord> iterator = records.iterator();
		long time1 = System.nanoTime();
		while (iterator.hasNext()) {

			for (int i = 0; i < recordsPerSlice; i++) {
				if (!iterator.hasNext())
					break;
				CramRecord record = iterator.next();
				codec.write(bos, record);
				counter++;
			}

			bos.close();
			block = new Block();
			block.contentType = ContentType.CORE.getContentType();
			block.contentId = 0;
			block.data = baos.toByteArray();
			block.method = CompressionMethod.GZIP.byteValue();
			blocks.add(block);
			baos.reset();
			for (Integer key : osMap.keySet()) {
				block = new Block();
				if (key > ContentType.TAG.getContentType()) {
					block.contentType = ContentType.TAG.getContentType();
					block.contentId = key.shortValue();
				} else {
					block.contentType = key.byteValue();
					block.contentId = 0;
				}

				MyDataOutputStream os = osMap.get(key);
				block.data = os.getData();
				os.resett();
				block.method = CompressionMethod.GZIP.byteValue();

				blocks.add(block);
			}
		}
		long time2 = System.nanoTime();
		System.out.printf("Written %d records in %.2f ms.\n", counter, (time2 - time1) / 1000000f);

		Container c = new Container();
		c.blocks = blocks.toArray(new Block[blocks.size()]);
		c.containers = new Container[0];
		c.contentId = 2;

		return c;
	}

	public List<CramRecord> readRecords(Container c, CramHeader cramHeader, SequenceBaseProvider referenceBaseProvider)
			throws IOException, CramFormatException, CramCompressionException {
		List<CramRecord> records = new ArrayList<CramRecord>();
		int b = 0;

		CramRecordBlockReader reader = new CramRecordBlockReader(new DataInputStream(new ByteArrayInputStream(
				c.blocks[b++].data)));
		CramRecordBlock crBlock = reader.read();

		CramRecordCodec codec = buildCodec(cramHeader, crBlock, referenceBaseProvider);
		List<Short> tagCodes = new ArrayList<Short>();
		tagCodes.add(SAMTagUtil.getSingleton().makeBinaryTag("OQ"));

		long counter = crBlock.getRecordCount();
		while (b < c.blocks.length) {
			Block block = c.blocks[b++];
			if (block.contentType != ContentType.CORE.getContentType())
				throw new RuntimeException("Wrong content type: " + block.contentType);
			if (block.contentId != 0)
				throw new RuntimeException("Wrong content id: " + block.contentId);
			BitInputStream bis = new DefaultBitInputStream(new ByteArrayInputStream(block.data));

			HashMap<Integer, DataInputStream> isMap = new HashMap<Integer, DataInputStream>();
			for (short code : tagCodes)
				isMap.put((int) code, new DataInputStream(new ByteArrayInputStream(c.blocks[b++].data)));
			isMap.put(0xff & ContentType.QUAL.getContentType(), new DataInputStream(new ByteArrayInputStream(c.blocks[b++].data)));

			codec.isMap = isMap;

			for (int r = 0; r < recordsPerSlice && counter-- > 0; r++) {
				try {
					CramRecord record = codec.read(bis);
					records.add(record);
				} catch (NullPointerException e) {
					System.out.println("Counter=" + counter);
					throw e;
				}
			}
		}

		return records;
	}

	private static class MyDataOutputStream extends DataOutputStream {
		private ByteArrayOutputStream baos;

		public MyDataOutputStream() {
			super(new ByteArrayOutputStream());
			baos = (ByteArrayOutputStream) out;
		}

		public byte[] getData() {
			return baos.toByteArray();
		}

		public void resett() {
			baos.reset();
		}

	}

	public static CramRecordCodec buildCodec(CramHeader cramHeader, CramRecordBlock crBlock,
			SequenceBaseProvider provider) throws CramCompressionException {
		RecordCodecFactory rcf = new RecordCodecFactory();
		DefaultMutableTreeNode rootNode = rcf.buildCodecTree(cramHeader, crBlock, provider);
		uk.ac.ebi.ena.sra.cram.encoding.MeasuringCodec mCodec = (MeasuringCodec) rootNode.getUserObject();
		uk.ac.ebi.ena.sra.cram.encoding.CramRecordCodec crCodec = (uk.ac.ebi.ena.sra.cram.encoding.CramRecordCodec) mCodec
				.getDelegate();
		CramRecordCodec codec = new CramRecordCodec();
		codec.baseCodec = crCodec.baseCodec;
		codec.defaultReadLength = crCodec.defaultReadLength;
		codec.flagsCodec = crCodec.flagsCodec;
		codec.heapByteCodec = crCodec.heapByteCodec;
		codec.inSeqPosCodec = crCodec.inSeqPosCodec;
		codec.mappingQualityCodec = crCodec.mappingQualityCodec;
		codec.nextFragmentIDCodec = crCodec.nextFragmentIDCodec;
		codec.preserveReadNames = crCodec.preserveReadNames;
		codec.prevPosInSeq = crCodec.prevPosInSeq;
		codec.qualityCodec = crCodec.qualityCodec;
		codec.qualityCodec = null;
		codec.readAnnoCodec = crCodec.readAnnoCodec;
		codec.readGroupCodec = crCodec.readGroupCodec;
		codec.readlengthCodec = crCodec.readlengthCodec;
		codec.readNameCodec = crCodec.readNameCodec;
		codec.recordsToNextFragmentCodec = crCodec.recordsToNextFragmentCodec;
		codec.sequenceBaseProvider = crCodec.sequenceBaseProvider;
		codec.storeMappedQualityScores = true;
		codec.tagCodecMap = crCodec.tagCodecMap;
		codec.tagCountCodec = crCodec.tagCountCodec;
		codec.tagKeyAndTypeCodec = crCodec.tagKeyAndTypeCodec;
		;
		codec.tagValueByteLenCodec = crCodec.tagValueByteLenCodec;
		codec.variationsCodec = crCodec.variationsCodec;

		return codec;
	}
}
