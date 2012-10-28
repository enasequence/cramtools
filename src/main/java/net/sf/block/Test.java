package net.sf.block;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

public class Test {

	public static void main(String[] args) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Definition definition = new Definition();
		definition.contentId = "input.bam".getBytes();
		definition.formatMajor = 1;
		definition.formatMinor = 0;
		definition.magick = "CRAM".getBytes();

		System.out.printf("Definition: magick=%s, major=%d, minor=%d, content id=%s\n", new String(definition.magick),
				definition.formatMajor, definition.formatMinor, definition.contentId);

		int len = 0 ;
		FormatFactory ff = new FormatFactory();
		len += ff.writeDefinition(definition, baos);
		Format format = ff.createFormat(definition);
		
//		System.out.println(baos.toString());

		Random r = new Random();

		int nofContainers = r.nextInt(5) + 1;
		for (int c = 0; c < nofContainers; c++) {
			Container container = new Container();
			container.containers = new Container[0];
			container.contentId = 1000L + c;

			int nofBlocks = r.nextInt(5);
			container.blocks = new Block[nofBlocks];
			for (int i = 0; i < nofBlocks; i++) {
				Block block = new Block();
				block.method = (byte) (r.nextBoolean() ? 0 : 2);
				block.contentType = (byte) i;
				block.data = "Test block body.".getBytes();
				container.blocks[i] = block;
			}
			len += format.writeContainer(container, baos);
		}
		
		System.out.printf("Written bytes=%d, baos size=%d\n", len, baos.size());
		
//		System.out.println(baos.toString());

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		definition = ff.readDefinition(bais);
		System.out.printf("Definition: magick=%s, major=%d, minor=%d, content id=%s\n", new String(definition.magick),
				definition.formatMajor, definition.formatMinor, definition.contentId);
		
		format = ff.createFormat(definition);
		Container container = null;
		while ((container = format.readContainer(bais)) != null) {
			System.out.printf("Container: %d, %d, %d, %d\n", container.contentId, container.length,
					container.blocks.length, container.containers.length);
			for (int i = 0; i < container.blocks.length; i++) {
				Block b = container.blocks[i];
				System.out.printf("CType=%d, method=%d, bytes=%d, dataBytes=%d, body=%s\n", b.contentType, b.method,
						b.bytes, b.dataBytes, new String(b.data));
			}
		}
	}

}
