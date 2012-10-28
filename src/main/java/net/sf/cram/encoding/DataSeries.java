package net.sf.cram.encoding;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.sf.cram.DataSeriesType;
import net.sf.cram.EncodingKey;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataSeries {
	EncodingKey key();
	DataSeriesType type() ;
}
