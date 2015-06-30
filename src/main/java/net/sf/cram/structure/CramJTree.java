package net.sf.cram.structure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;

import net.sf.cram.build.CramIO;
import net.sf.cram.common.IntHashMap;
import net.sf.cram.encoding.DataSeries;
import net.sf.cram.encoding.DataSeriesMap;
import net.sf.cram.encoding.DataSeriesType;
import net.sf.cram.encoding.Encoding;
import net.sf.cram.encoding.EncodingFactory;
import net.sf.cram.encoding.ExternalByteArrayEncoding;
import net.sf.cram.encoding.ExternalByteEncoding;
import net.sf.cram.encoding.ExternalIntegerEncoding;
import net.sf.cram.encoding.ExternalLongEncoding;
import net.sf.cram.encoding.NullEncoding;
import net.sf.cram.encoding.reader.AbstractReader;
import net.sf.cram.io.BitInputStream;
import net.sf.cram.io.DefaultBitInputStream;
import net.sf.picard.util.Log;

/**
 * Created by vadim on 22/01/2015.
 */
public class CramJTree extends JTree {

	public CramJTree(String name, InputStream is) throws IOException, IllegalAccessException {
		super();
		Log.setGlobalLogLevel(Log.LogLevel.ERROR);
		DefaultMutableTreeNode root = new DefaultMutableTreeNode(name);
		final CramHeader cramHeader = CramIO.readCramHeader(is);

		root.add(new DefaultMutableTreeNode(cramHeader));

		while (true) {
			final Container container = CramIO.readContainer(cramHeader, is);

			if (container.isEOF()) {
				root.add(createContainerNode(container));
				break;
			}

			boolean unmapped = container.sequenceId >= 0;
			if (root.getChildCount() < 5)
				root.add(createContainerNode(container));
			else {
				if (unmapped && root.getChildCount() < 10)
					root.add(createContainerNode(container));
			}

		}

		DefaultTreeModel model = new DefaultTreeModel(root);
		setModel(model);
	}

	private DefaultMutableTreeNode createContainerNode(final Container container) {
		final DefaultMutableTreeNode containerNode = new DefaultMutableTreeNode(container);
		containerNode.add(new DefaultMutableTreeNode(container.h));

		if (container.slices != null) {
			for (Slice slice : container.slices) {
				DefaultMutableTreeNode sliceNode = new DefaultMutableTreeNode(slice);
				sliceNode.add(new DefaultMutableTreeNode(slice.coreBlock));
				sliceNode.add(new DefaultMutableTreeNode(slice.headerBlock));
				if (slice.embeddedRefBlock != null)
					sliceNode.add(new DefaultMutableTreeNode(slice.embeddedRefBlock));

				for (Block block : slice.external.values())
					sliceNode.add(new DefaultMutableTreeNode(block));

				DefaultMutableTreeNode byEncodingNode = new DefaultMutableTreeNode("Data series");

				{
					final Map<EncodingKey, Encoding> nonTagEncodings = createNonTagEncodings(container, slice);

					for (Map.Entry<EncodingKey, Encoding> entry : nonTagEncodings.entrySet()) {
						DefaultMutableTreeNode encodingKeyNode = new DefaultMutableTreeNode(entry.getKey());
						DefaultMutableTreeNode encodingNode = new DefaultMutableTreeNode(entry.getValue());
						encodingKeyNode.add(encodingNode);

						Encoding e = entry.getValue();
						if (e.id() == EncodingID.EXTERNAL) {
							int contentID = -1;
							if (e instanceof ExternalByteEncoding)
								contentID = ((ExternalByteEncoding) e).contentId;
							if (e instanceof ExternalByteArrayEncoding)
								contentID = ((ExternalByteArrayEncoding) e).contentId;
							if (e instanceof ExternalIntegerEncoding)
								contentID = ((ExternalIntegerEncoding) e).contentId;
							if (e instanceof ExternalLongEncoding)
								contentID = ((ExternalLongEncoding) e).contentId;
							if (contentID >= 0) {
								DefaultMutableTreeNode blockNode = new DefaultMutableTreeNode(
										slice.external.get(contentID));
								encodingKeyNode.removeAllChildren();
								encodingKeyNode.add(blockNode);
							}
						}

						byEncodingNode.add(encodingKeyNode);
					}
				}

				DefaultMutableTreeNode byTagNode = new DefaultMutableTreeNode("Tags");
				{

					final Map<Integer, Encoding> tagEncodings = createTagEncodings(container, slice);
					for (Map.Entry<Integer, Encoding> entry : tagEncodings.entrySet()) {
						String tagName = ReadTag.intToNameType4Bytes(entry.getKey());
						DefaultMutableTreeNode tagNode = new DefaultMutableTreeNode(tagName);
						DefaultMutableTreeNode encodingNode = new DefaultMutableTreeNode(entry.getValue());
						tagNode.add(encodingNode);
						Encoding e = entry.getValue();
						if (e.id() == EncodingID.EXTERNAL) {
							int contentID = -1;
							if (e instanceof ExternalByteEncoding)
								contentID = ((ExternalByteEncoding) e).contentId;
							if (e instanceof ExternalByteArrayEncoding)
								contentID = ((ExternalByteArrayEncoding) e).contentId;
							if (e instanceof ExternalIntegerEncoding)
								contentID = ((ExternalIntegerEncoding) e).contentId;
							if (e instanceof ExternalLongEncoding)
								contentID = ((ExternalLongEncoding) e).contentId;
							if (contentID >= 0) {
								DefaultMutableTreeNode blockNode = new DefaultMutableTreeNode(
										slice.external.get(contentID));
								encodingNode.add(blockNode);
							}
						}

						byTagNode.add(tagNode);
					}
				}

				containerNode.add(byEncodingNode);
				containerNode.add(byTagNode);
				containerNode.add(sliceNode);
			}
		}
		return containerNode;
	}

	@Override
	public String convertValueToText(Object value, final boolean selected, final boolean expanded, final boolean leaf,
			final int row, final boolean hasFocus) {
		if (value == null)
			return "";

		if (value instanceof TreeNode) {
			value = ((DefaultMutableTreeNode) value).getUserObject();
			if (value instanceof CramHeader) {
				CramHeader c = (CramHeader) value;
				String label = String.format("Header: version %d.%d; id: %s", c.getMajorVersion(), c.getMinorVersion(),
						new String(c.getId()));
				return label;
			}

			if (value instanceof Container) {
				Container c = (Container) value;
				if (c.isEOF())
					return "EOF";
				return String
						.format("Container %d:%d-%d/%d", c.sequenceId, c.alignmentStart, c.alignmentSpan, c.offset);
			}

			if (value instanceof CompressionHeader) {
				CompressionHeader c = (CompressionHeader) value;
				return String.format("Compression header");
			}

			if (value instanceof Slice) {
				Slice slice = (Slice) value;
				return String.format("Slice %d:%d-%d/%d", slice.sequenceId, slice.alignmentStart, slice.alignmentSpan,
						slice.offset);
			}

			if (value instanceof Block) {
				Block block = (Block) value;
				switch (block.contentType) {
				case COMPRESSION_HEADER:
					return String.format("Compression header");
				case FILE_HEADER:
					return String.format("File header");
				case MAPPED_SLICE:
					return String.format("Mapped slice");
				case CORE:
					return String.format("Core block");
				case EXTERNAL:
					return String.format("External %d/%s", block.contentId, block.method.name());
				default:
					return "unknown block";
				}

			}
		}

		if (value instanceof EncodingKey) {
			EncodingKey key = (EncodingKey) value;
			return String.format("%s", key.name());
		}

		if (value instanceof Encoding) {
			Encoding key = (Encoding) value;
			return String.format("%s", key.id().name());
		}
		return super.convertValueToText(value, selected, expanded, leaf, row, hasFocus);
	}

	private static class CramModel extends DefaultTreeModel {

		public CramModel(final TreeNode root) {
			super(new DefaultMutableTreeNode("CRAM"), true);
			getRoot();
		}
	}

	public static TreeModel buildTreeModel(InputStream is) {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("CRAM");
		DefaultTreeModel model = new DefaultTreeModel(root);
		return model;
	}

	private static Map<EncodingKey, Encoding> createNonTagEncodings(Container c, Slice s) {

		CompressionHeader h = c.h;

		BitInputStream bis = new DefaultBitInputStream(new ByteArrayInputStream(s.coreBlock.getRawContent()));

		Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
		for (Integer exId : s.external.keySet()) {
			inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId).getRawContent()));
		}

		Map<EncodingKey, Encoding> encodingMap = new HashMap<EncodingKey, Encoding>();
		for (Field f : AbstractReader.class.getFields()) {
			if (f.isAnnotationPresent(DataSeries.class)) {
				DataSeries ds = f.getAnnotation(DataSeries.class);
				EncodingKey key = ds.key();
				DataSeriesType type = ds.type();
				assert (h.eMap.get(key) != null);
				Encoding e = createEncoding(type, h.eMap.get(key), bis, inputMap);
				encodingMap.put(key, e);
			}

			if (f.isAnnotationPresent(DataSeriesMap.class)) {
				DataSeriesMap dsm = f.getAnnotation(DataSeriesMap.class);
				String name = dsm.name();
				if ("TAG".equals(name)) {
					IntHashMap map = new IntHashMap();
					for (Integer key : h.tMap.keySet()) {
						EncodingParams params = h.tMap.get(key);
						final Encoding<Object> encoding = createEncoding(DataSeriesType.BYTE_ARRAY, params, bis,
								inputMap);
					}
				}
			}
		}

		return encodingMap;
	}

	private static Map<Integer, Encoding> createTagEncodings(Container c, Slice s) {

		CompressionHeader h = c.h;

		BitInputStream bis = new DefaultBitInputStream(new ByteArrayInputStream(s.coreBlock.getRawContent()));

		Map<Integer, InputStream> inputMap = new HashMap<Integer, InputStream>();
		for (Integer exId : s.external.keySet()) {
			inputMap.put(exId, new ByteArrayInputStream(s.external.get(exId).getRawContent()));
		}

		Map<Integer, Encoding> encodingMap = new HashMap<Integer, Encoding>();
		for (Field f : AbstractReader.class.getFields()) {
			if (f.isAnnotationPresent(DataSeriesMap.class)) {
				DataSeriesMap dsm = f.getAnnotation(DataSeriesMap.class);
				String name = dsm.name();
				if ("TAG".equals(name)) {
					IntHashMap map = new IntHashMap();
					for (Integer key : h.tMap.keySet()) {
						EncodingParams params = h.tMap.get(key);
						final Encoding<Object> encoding = createEncoding(DataSeriesType.BYTE_ARRAY, params, bis,
								inputMap);
						encodingMap.put(key, encoding);
					}
				}
			}
		}

		return encodingMap;
	}

	private static <T> Encoding<T> createEncoding(DataSeriesType valueType, EncodingParams params, BitInputStream bis,
			Map<Integer, InputStream> inputMap) {
		if (params.id == EncodingID.NULL)
			return new NullEncoding<T>();

		EncodingFactory f = new EncodingFactory();
		Encoding<T> encoding = f.createEncoding(valueType, params.id);
		if (encoding == null)
			throw new RuntimeException("Encoding not found for value type " + valueType.name() + ", id=" + params.id);
		encoding.fromByteArray(params.params);

		return encoding;
	}
}