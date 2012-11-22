package net.sf.cram;

import java.io.File;
import java.util.Scanner;

public class CramProf {
	public static void main(String[] args) throws Exception {
		String paramsFilePath = System.getProperty("CramParamsFile", "options");
		File file = new File(paramsFilePath);

		Scanner scanner = new Scanner(file);
		String paramLine = scanner.nextLine();
		scanner.close() ;
		String[] params = paramLine.split("\\s+");
		for (String param : params)
			System.err.println(param);

		CramTools.main(params);
	}
}
