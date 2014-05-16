package net.sf.cram.cg;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

class Splitter {

	static void split(Read.HalfDnb half) {

		int sequencePosition = 0;
		int skipSequenceLen = 0;

		for (CigEl e : half.cgCigElList) {
			switch (e.op) {
			case '=':
			case 'X':
			case 'M':
			case 'I':
				if (skipSequenceLen > 0) {
					if (e.len <= skipSequenceLen) {
						skipSequenceLen -= e.len;
						sequencePosition += e.len;
						break;
					} else {
						half.gcList.add(new CigEl(e.len - skipSequenceLen, 'S'));
						e.len -= skipSequenceLen;
						sequencePosition += skipSequenceLen;
						skipSequenceLen = 0;
					}
				} else {
					half.gcList.add(new CigEl(e.len, 'S'));
				}

				for (int i = 0; i < e.len; i++) {
					half.readBasesBuf.put(half.baseBuf.get(sequencePosition + i));
					half.readScoresBuf.put(half.scoreBuf.get(sequencePosition + i));
				}
				sequencePosition += e.len;

			case 'N':
				add(half.samCigarElements, e);
				break;

			case 'P':
			case 'D':
				if (skipSequenceLen == 0)
					add(half.samCigarElements, e);
				break;

			case 'B':
				half.readBasesBuf.position(half.readBasesBuf.position() - e.len);
				half.readScoresBuf.position(half.readScoresBuf.position() - e.len);
				int startPos = sequencePosition - e.len;
				for (int i = 0; i < e.len; ++i) {
					byte scoreL = half.scoreBuf.get(startPos + i);
					byte scoreR = half.scoreBuf.get(sequencePosition + i);
					if (scoreL > scoreR) {
						half.readBasesBuf.position(half.readScoresBuf.position() + 1);
						half.readScoresBuf.position(half.readScoresBuf.position() + 1);
					} else {
						half.readBasesBuf.put(half.baseBuf.get(sequencePosition + i));
						half.readScoresBuf.put(scoreR);
					}
				}
				for (int i = 0; i < e.len * 2; i++) {
					half.gsBuf.put(half.baseBuf.get(startPos + i));
					half.gqBuf.put(half.scoreBuf.get(startPos + i));

				}
				half.gcList.get(half.gcList.size() - 1).len -= e.len;
				half.gcList.add(new CigEl(e.len, 'G'));
				skipSequenceLen = e.len;
				break;
			default:
				break;
			}
		}
	}

	private static final char collapsibleType(char ch) {
		switch (ch) {
		case 'I':
		case 'P':
			return 'I';
		case 'D':
			return 'D';
		default:
			return 0;
		}
	}

	static LinkedList<CigEl> pack(List<CigEl> list) {
		LinkedList<CigEl> result = new LinkedList<CigEl>();

		for (CigEl e : list) {
			add(result, e);
			int prevIndex = result.size() - 1;
			while (prevIndex > 0) {
				CigEl lastE = result.get(prevIndex);
				char lastType = collapsibleType(lastE.op);
				if (lastType > 0) {
					--prevIndex;
					CigEl prevE = result.get(prevIndex);
					char prevType = collapsibleType(prevE.op);
					if (prevType == 0 || lastType == prevType)
						break;

					// collapse insertion and deletion and generate 'M'/'N' to
					// replace the overlapping part
					boolean prevEShorter = prevE.len < lastE.len;
					CigEl shortE = prevEShorter ? prevE : lastE;
					CigEl longE = !prevEShorter ? prevE : lastE;
					longE.len -= shortE.len;
					if (longE.op == 'I' || shortE.op == 'I') {
						shortE.op = 'M';
					} else {
						shortE.op = 'N';
					}
					if (prevE.len == 0) {
						result.remove(prevIndex);
						break;
					} else if (prevIndex > 0) {
						CigEl checkE = result.get(prevIndex - 1);
						if (checkE.op == prevE.op) {
							checkE.len += prevE.len;
							result.remove(prevIndex);
						} else {
							Collections.swap(result, prevIndex, prevIndex + 1);
						}
					}
				} else
					break;
			}
		}
		return result;
	}

	private static final void add(List<CigEl> list, CigEl e) {
		add(list, e.len, e.op);
	}

	private static final void add(List<CigEl> list, int len, char op) {
		if (len == 0)
			return;
		if (!list.isEmpty()) {
			CigEl last = list.get(list.size() - 1);
			if (last.op == op) {
				last.len += len;
				return;
			}
		}
		list.add(new CigEl(len, op));
	}
}
