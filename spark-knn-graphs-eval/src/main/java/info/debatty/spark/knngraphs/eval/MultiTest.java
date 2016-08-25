/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package info.debatty.spark.knngraphs.eval;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import info.debatty.spark.knngraphs.builder.Online;
import info.debatty.spark.knngraphs.builder.StatisticsAccumulator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class MultiTest<T> {

    public static final int NODES_BEFORE_FEEDBACK = 10;

    public int n;
    public int n_add;
    public int n_evaluation;
    public int k = 10;

    public LinkedList<Batch> batches = new LinkedList<Batch>();

    private int partitioning_iterations;
    private int partitioning_medoids;
    private double search_speedup;
    private int search_random_jumps;
    private double search_expansion;
    private int update_depth;
    private double medoids_update_ratio = 0;
    private String result_file;

    private SimilarityInterface<T> similarity;
    public Iterator<T> dataset_iterator;

    private JavaPairRDD<Node<T>, NeighborList> initial_graph;
    private JavaPairRDD<Node<T>,NeighborList> exact_graph;
    private ArrayList<Node<T>> test_dataset;

    private PrintWriter result_file_writer;
    private JavaSparkContext sc;

    public final void run() throws Exception {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        log("Configure spark instance");
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        sc = new JavaSparkContext(conf);
        log("Spark version: " + sc.version());

        log("Read dataset...");
        List<Node<T>> dataset = new ArrayList<Node<T>>(n + n_add);
        for (int i = 0; i < n + n_add; i++) {
            dataset.add(new Node<T>(
                    String.valueOf(i),
                    dataset_iterator.next()));
        }

        log("Compute verification graph");
        DistributedGraphBuilder<T> builder = new Brute<T>();
        builder.setK(k);
        builder.setSimilarity(similarity);
        exact_graph
                = builder
                        .computeGraph(sc.parallelize(dataset))
                        .persist(StorageLevel.DISK_ONLY());
        exact_graph.count();
        log("done...");

        log("Split the dataset between training and test...");
        Random rand = new Random();
        test_dataset = new ArrayList<Node<T>>(n_add);
        for (int i = 0; i < n_add; i++) {
            test_dataset.add(
                    dataset.remove(rand.nextInt(dataset.size())));
        }

        log("Compute initial graph...");
        initial_graph = builder.computeGraph(sc.parallelize(dataset));
        initial_graph = initial_graph.cache();
        initial_graph.count();
        log("done...");

        for (Batch batch : batches) {
            runBatch(batch, initial_graph, test_dataset);
        }

    }

    private void runBatch(
            Batch batch,
            JavaPairRDD<Node<T>, NeighborList> initial_graph,
            ArrayList<Node<T>> test_dataset) throws IOException {

        result_file = batch.result_file;
        if (!result_file.equals("-")) {
            log("Create result file");
            File f = new File(result_file);
            if (f.exists()) {
                result_file_writer = new PrintWriter(
                        new BufferedWriter(new FileWriter(result_file, true)));

            } else {
                result_file_writer = new PrintWriter(
                        new BufferedWriter(new FileWriter(result_file, true)));
                writeHeader();
            }
        }

        for (int partitioning_medoids : batch.partitioning_medoids) {
            this.partitioning_medoids = partitioning_medoids;

            for (int partitioning_iterations : batch.partitioning_iterations) {
                this.partitioning_iterations = partitioning_iterations;

                for (int update_depth : batch.update_depths) {
                    this.update_depth = update_depth;

                    for (int search_random_jumps : batch.random_jumps) {
                        this.search_random_jumps = search_random_jumps;

                        for (double search_speedup : batch.search_speedups) {
                            this.search_speedup = search_speedup;

                            for (double search_expansion : batch.search_expansions) {
                                this.search_expansion = search_expansion;
                                printSettings();
                                runTest();
                                System.out.println(
                                        "Number of RDD's in cluster: "
                                        + sc.sc().getPersistentRDDs().size());
                            }
                        }
                    }
                }
            }
        }
    }

    private void runTest() throws IOException {

        log("Initialize online graph (partition)...");
        long start_time = System.currentTimeMillis();
        Online<T> online_graph = new Online<T>(
                k,
                similarity,
                sc,
                initial_graph,
                partitioning_medoids,
                partitioning_iterations);

        online_graph.setUpdateDepth(update_depth);
        online_graph.setSearchSpeedup(search_speedup);
        online_graph.setSearchRandomJumps(search_random_jumps);
        online_graph.setSearchExpansion(search_expansion);
        online_graph.setMedoidUpdateRatio(0);

        long time_partition_graph = System.currentTimeMillis() - start_time;
        log("DONE!");
        System.out.printf(
                "%-30s %d ms\n", "Time to partition graph:",
                time_partition_graph);

        log("Add nodes...");
        int i = 0;
        long similarities = 0;
        long restarts = 0;
        // search restarts due to cross partition edges
        long xpartition_restarts = 0;
        start_time = System.currentTimeMillis();
        for (final Node<T> query : test_dataset) {
            i++;
            Accumulator<StatisticsContainer> stats_accumulator = sc.accumulator(
                    new StatisticsContainer(),
                    new StatisticsAccumulator());

            online_graph.fastAdd(query, stats_accumulator);

            StatisticsContainer global_stats = stats_accumulator.value();
            similarities += global_stats.getSimilarities();
            restarts += global_stats.getSearchRestarts();
            xpartition_restarts
                    += global_stats.getSearchCrossPartitionRestarts();

            if (i % NODES_BEFORE_FEEDBACK == 0) {
                log("" + i);
            }
        }

        long correct = info.debatty.spark.knngraphs.Graph.countCommonEdges(
                exact_graph,
                online_graph.getGraph());
        online_graph.clean();

        System.out.printf(
                "%-30s %d (%f)\n", "Correct edges in online graph: ",
                correct, 1.0 * correct / (k * (n + i)));

        long time_add = System.currentTimeMillis() - start_time;
        writeResult(
                i,
                correct,
                similarities,
                time_add,
                restarts,
                xpartition_restarts);

    }

    protected final void printSettings() {

        System.out.printf("%-30s %s\n", "Initial graph size:", n);
        System.out.printf("%-30s %d\n", "k:", k);
        System.out.printf("%-30s %d\n", "Partitioning iterations:",
                partitioning_iterations);
        System.out.printf("%-30s %d\n", "Partitioning medoids:",
                partitioning_medoids);
        System.out.printf("%-30s %d\n", "Update depth:",
                update_depth);
        System.out.printf("%-30s %d\n", "Nodes to add:",
                n_add);
        System.out.printf("%-30s %f\n", "Search speedup:", search_speedup);
        System.out.printf("%-30s %d\n", "Search random jumps:",
                search_random_jumps);
        System.out.printf("%-30s %f\n", "Search expansion:", search_expansion);
        System.out.printf("%-30s %f\n", "Medoid update ratio:",
                medoids_update_ratio);
        System.out.printf("%-30s %s\n", "Result file:", result_file);
    }

    public final void setSimilarity(final SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    protected final void log(final String s) {
        java.util.Date date = new java.util.Date();
        SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
        System.out.printf("%s %s\n", format.format(date), s);
    }

    private void writeHeader() {
        result_file_writer.printf("# n\t");
        result_file_writer.printf("k\t");
        result_file_writer.printf("n_evaluation\t");
        result_file_writer.printf("n_added\t");
        result_file_writer.printf("partitioning_iterations\t");
        result_file_writer.printf("partitioning_medoids\t");
        result_file_writer.printf("update_depth\t");
        result_file_writer.printf("medoids_update_ratio\t");
        result_file_writer.printf("search_speedup\t");
        result_file_writer.printf("search_random_jumps\t");
        result_file_writer.printf("search_expansion\t");
        result_file_writer.printf("correct\t");
        result_file_writer.printf("correct_ratio\t");
        result_file_writer.printf("quality\t");
        result_file_writer.printf("similarities\t");
        result_file_writer.printf("real_speedup\t");
        result_file_writer.printf("quality_equivalent_speedup\t");
        result_file_writer.printf("time_add\t");
        result_file_writer.printf("restarts\t");
        result_file_writer.printf("xpartition_restarts\n");
        result_file_writer.flush();
    }

    private Graph<T> list2graph(
            final List<Tuple2<Node<T>, NeighborList>> list) {

        Graph<T> graph = new Graph<T>();
        for (Tuple2<Node<T>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }

    private void writeResult(
            final int n_added,
            final long correct,
            final long similarities,
            final long time_add,
            final long restarts,
            final long xpartition_restarts) throws IOException {

        // Write results to file
        if (result_file_writer == null) {
            return;
        }

        double em = 1.0 * n * k * n_added / (n_added + n) + n_added * k;
        double eu = (1.0 * n + n_added) * k - em;
        double quality = (1.0 * correct - eu) / em;
        System.out.println("Quality factor (Q): " + quality);

        double correct_ratio = 1.0 * correct / (k * (n_added + n));

        long n_end = n + n_added;
        long n_start = n;
        long similarities_naive
                = n_end * (n_end - 1) / 2 - n_start * (n_start - 1) / 2;

        double real_speedup = 1.0 * similarities_naive / similarities;
        double quality_equivalent_speedup = quality * real_speedup;

        result_file_writer.printf("%d\t", n);
        result_file_writer.printf("%d\t", k);
        result_file_writer.printf("%d\t", n_evaluation);
        result_file_writer.printf("%d\t", n_added);
        result_file_writer.printf("%d\t", partitioning_iterations);
        result_file_writer.printf("%d\t", partitioning_medoids);
        result_file_writer.printf("%d\t", update_depth);
        result_file_writer.printf("%f\t", medoids_update_ratio);
        result_file_writer.printf("%f\t", search_speedup);
        result_file_writer.printf("%d\t", search_random_jumps);
        result_file_writer.printf("%f\t", search_expansion);
        result_file_writer.printf("%d\t", correct);
        result_file_writer.printf("%f\t", correct_ratio);
        result_file_writer.printf("%f\t", quality);
        result_file_writer.printf("%d\t", similarities);
        result_file_writer.printf("%f\t", real_speedup);
        result_file_writer.printf("%f\t", quality_equivalent_speedup);
        result_file_writer.printf("%d\t", time_add);
        result_file_writer.printf("%d\t", restarts);
        result_file_writer.printf("%d\n", xpartition_restarts);
        result_file_writer.flush();
    }
}
