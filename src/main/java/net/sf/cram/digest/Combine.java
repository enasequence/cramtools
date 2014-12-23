package net.sf.cram.digest;

interface Combine<T> {

	T combine(T state, T update);
}
