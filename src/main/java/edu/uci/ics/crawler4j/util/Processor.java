package edu.uci.ics.crawler4j.util;

public abstract class Processor<T, R> {
  public abstract R apply(T arg);
}