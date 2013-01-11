package net.sf.cram;

public class AlignmentSliceQuery {
	public String sequence;
	public int start;
	public int end;

	public AlignmentSliceQuery(String spec) {
		String[] chunks = spec.split(":");

		sequence = chunks[0];

		if (chunks.length > 1) {
			chunks = chunks[1].split("-");
			start = Integer.valueOf(chunks[0]);
			if (chunks.length == 2)
				end = Integer.valueOf(chunks[1]);
		}

	}
}
