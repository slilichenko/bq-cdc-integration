package com.google.demo.bigquery;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

class StructTest {
  @org.junit.jupiter.api.Test
  void addString() {
    Struct struct = new Struct();
    struct.addString("abc", "xyz");
    struct.addString("dce", "Poor man's test");
    assertEquals("STRUCT('xyz' as abc,'Poor man\\'s test' as dce)", struct.toStructConstant());
  }

  @org.junit.jupiter.api.Test
  void addTimestampInMicroseconds() {
    Struct struct = new Struct();
    struct.addTimestamp("abc", Instant.ofEpochSecond( 1571068536L, 842*1000_000));
    assertEquals("STRUCT(TIMESTAMP('2019-10-14 15:55:36.842000+00:00') as abc)", struct.toStructConstant());
  }
}