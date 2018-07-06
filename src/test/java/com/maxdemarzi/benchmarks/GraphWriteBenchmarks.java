package com.maxdemarzi.benchmarks;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GraphWriteBenchmarks {

    private GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    private static final Label LABEL = Label.label("Item");
    private static final String KEY = "key";
    private static final Long LONG = 1L;
    private static final String SHORT_STRING = "short";
    private static final String LONG_STRING = "This is a long string going over 41 characters.";

    @Param({"100000"})
    private int itemCount;

    @Setup(Level.Iteration)
    public void prepare() {
        db.shutdown();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Transaction tx = db.beginTx();
        //This creates the tokens for label and property key.
        Node itemNode = db.createNode(LABEL);
        itemNode.setProperty(KEY, LONG_STRING);
        tx.success();
        tx.close();
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureWriteUnlabeled() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                db.createNode();
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureWriteLabeled() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                db.createNode(LABEL);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureWriteLabeledWithLongProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, LONG);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureWriteLabeledWithShortStringProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, SHORT_STRING);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureWriteLabeledWithLongStringProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, LONG_STRING);
            }
            tx.success();
        }
        return items;
    }

    // Multiple Threads
    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureMTWriteUnlabeled() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                db.createNode();
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureMTWriteLabeled() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                db.createNode(LABEL);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureMTWriteLabeledWithLongProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, LONG);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureMTWriteLabeledWithShortStringProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, SHORT_STRING);
            }
            tx.success();
        }
        return items;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureMTWriteLabeledWithLongStringProperty() {
        int items = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                items++;
                Node item = db.createNode(LABEL);
                item.setProperty(KEY, LONG_STRING);
            }
            tx.success();
        }
        return items;
    }
}
