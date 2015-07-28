/*******************************************************************************
 * Copyright 2013 EMBL-EBI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package net.sf.cram;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Log;
import net.sf.cram.common.Utils;
import net.sf.cram.index.CramIndexer;

public class CramTools {
	private static Log log = Log.getInstance(CramTools.class);

	private static Map<String, Class<?>> classes = new TreeMap<String, Class<?>>();

	private static Field findStaticField(String name, Class<?> klass) {
		Field[] declaredFields = klass.getDeclaredFields();
		Field field = null;
		for (Field f : declaredFields) {
			if (java.lang.reflect.Modifier.isStatic(f.getModifiers()) && f.getName().equals(name)) {
				field = f;
				break;
			}
		}

		return field;
	}

	private static Method findStaticMainMethod(Class<?> klass) {
		Method[] declaredFields = klass.getDeclaredMethods();
		Method mainMethod = null;
		for (Method m : declaredFields) {
			if (java.lang.reflect.Modifier.isStatic(m.getModifiers()) && m.getName().equals("main")) {
				mainMethod = m;
				break;
			}
		}

		return mainMethod;
	}

	private static Class<?> findParamsClass(Class<?> klass) {
		Class<?> paramsClass = null;
		for (Class<?> subClass : klass.getDeclaredClasses()) {
			if ("Params".equals(subClass.getSimpleName())) {
				paramsClass = subClass;
				break;
			}
		}
		return paramsClass;
	}

	private static void addProgram(JCommander jc, Class<?> klass) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		Class<?> paramsClass = findParamsClass(klass);
		Object instance = paramsClass.newInstance();

		Field commandField = findStaticField("COMMAND", klass);
		String command = commandField.get(null).toString();

		jc.addCommand(command, instance);
		classes.put(command, klass);
	}

	private static void invoke(String command, String[] args) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Method mainMethod = findStaticMainMethod(classes.get(command));
		mainMethod.invoke(null, (Object) args);
	}

	public static void main(String[] args) throws Exception {

		Params params = new Params();
		JCommander jc = new JCommander(params);
		jc.setProgramName("cramtools");

		addProgram(jc, Cram2Bam.class);
		addProgram(jc, Bam2Cram.class);
		addProgram(jc, CramIndexer.class);
		addProgram(jc, Merge.class);
		addProgram(jc, Cram2Fastq.class);
		addProgram(jc, CramFixHeader.class);
		addProgram(jc, DownloadReferences.class);
		addProgram(jc, QualityScoreStats.class);

		jc.parse(args);

		String command = jc.getParsedCommand();

		if (command == null || params.help) {
			Utils.printUsage(jc);
			return;
		}

		String[] commandArgs = new String[args.length - 1];
		System.arraycopy(args, 1, commandArgs, 0, commandArgs.length);

		invoke(command, commandArgs);
	}

	@Parameters(commandDescription = "CRAM tools. ")
	private static class Params {
		@Parameter(names = { "-h", "--help" }, description = "Print help and quit")
		private boolean help = false;
	}

	public static class LevelConverter implements IStringConverter<Log.LogLevel> {

		@Override
		public Log.LogLevel convert(String s) {
			return Log.LogLevel.valueOf(s.toUpperCase());
		}

	}

	public static class ValidationStringencyConverter implements IStringConverter<ValidationStringency> {

		@Override
		public ValidationStringency convert(String s) {
			return ValidationStringency.valueOf(s.toUpperCase());
		}
	}
}
