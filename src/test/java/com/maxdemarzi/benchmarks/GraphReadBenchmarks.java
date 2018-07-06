package com.maxdemarzi.benchmarks;

import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GraphReadBenchmarks {

    private GraphDatabaseService db;
    private Random rand = new Random();

    @Param({"1000"})
    private int userCount;

    @Param({"100"})
    private int personCount;

    @Param({"20000"})
    private int itemCount;

    @Param({"100"})
    private int friendsCount;

    @Param({"100"})
    private int likesCount;

    @Setup(Level.Iteration)
    public void prepare() throws IOException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        int count = 0;
        Transaction tx = db.beginTx();
        try {
            for (int item = 0; item < itemCount; item++) {
                Node itemNode = db.createNode(Label.label("Item"));
                itemNode.setProperty("id", item);
                itemNode.setProperty("itemname", "itemname" + item);
                if(count++ % 1_000 == 0){
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            for (int person = 0; person < personCount; person++) {
                Node personNode = db.createNode(Label.label("Person"));
                personNode.setProperty("id", "person" + person);
                for (int like = 0; like < likesCount; like++) {
                    personNode.createRelationshipTo(db.getNodeById(rand.nextInt(itemCount)), RelationshipType.withName("LIKES"));
                    if(count++ % 1_000 == 0){
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        tx.success();
    } finally {
        tx.close();
    }
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureTraverse() throws IOException {
        int person = 0;
        try (Transaction tx = db.beginTx()) {
            for (int i = itemCount; i < itemCount + personCount; i++) {
                person++;
                Node personNode = db.getNodeById(i);
                for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                    person++;
                }
            }
            tx.success();
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureTraverseViaLabel() throws IOException {
        int person = 0;
        try (Transaction tx = db.beginTx()) {
            Iterator<Node> it = db.findNodes(Label.label("Person"));
            while (it.hasNext()) {
                person++;
                Node personNode = it.next();
                for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                    person++;
                }
            }
            tx.success();
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureTraverseAndGetNodes() throws IOException {
        int person = 0;
        try (Transaction tx = db.beginTx()) {
            Iterator<Node> it = db.findNodes(Label.label("Person"));
            while (it.hasNext()) {
                person++;
                Node personNode = it.next();
                for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                    r.getEndNode().getAllProperties();
                    person++;
                }
            }
            tx.success();
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureRandomSingleTraversalIds() throws IOException {
        int person = 0;
        try (Transaction tx = db.beginTx()) {
            Node personNode = db.getNodeById(itemCount + rand.nextInt(personCount));
            for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                r.getEndNodeId();
                person++;
            }
        }
        return person;
    }


    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureFixedSingleTraversalIds() throws IOException {
        int person = 0;
        try (Transaction tx = db.beginTx()) {
            Node personNode = db.getNodeById(itemCount + 1);
            for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                r.getEndNodeId();
                person++;
            }
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureFixedSingleTraversalAndGetNodes() throws IOException {
        try (Transaction tx = db.beginTx()) {
            Node personNode = db.getNodeById(itemCount + 1);
            for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                r.getEndNode().getAllProperties();
            }
        }
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureSingleTraversalAndGetNodes() throws IOException {
        try (Transaction tx = db.beginTx()) {
            Node personNode = db.getNodeById(itemCount + rand.nextInt(personCount));

            for (Relationship r : personNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                r.getEndNode().getAllProperties();
            }
        }
    }
}
