package net.sf.block;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class FormatFactory {
	public static final int magicLength = 4;
	public static final int contentIdLength = 20;

	public Definition readDefinition(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		Definition definition = new Definition();
		definition.magick = new byte[magicLength];
		dis.readFully(definition.magick);

		definition.formatMajor = dis.readByte();
		definition.formatMinor = dis.readByte();

		definition.contentId = new byte[contentIdLength];
		dis.readFully(definition.contentId);
		return definition;
	}

	public int writeDefinition(Definition definition, OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);

		if (definition.magick.length > magicLength)
			throw new RuntimeException("Definition: magick must be less than " + magicLength + " bytes long.");

		byte[] magick = new byte[magicLength];
		Arrays.fill(magick, (byte) 0);
		System.arraycopy(definition.magick, 0, magick, 0, Math.min(definition.magick.length, magicLength));
		dos.write(magick);

		dos.write(definition.formatMajor);
		dos.write(definition.formatMinor);

		if (definition.contentId.length > contentIdLength)
			throw new RuntimeException("Definition: content id must be less than " + contentIdLength + " bytes long.");
		byte[] contentId = new byte[contentIdLength];
		Arrays.fill(contentId, (byte) 0);
		System.arraycopy(definition.contentId, 0, contentId, 0, Math.min(definition.contentId.length, contentIdLength));
		dos.write(contentId);

		dos.flush();
		return dos.size();
	}

	public Format createFormat(Definition definition) {
		return new Format(definition);
	}
}
