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

package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.Dijkstra;
import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class BalancedKMedoidsPartitioner<T>  {

    public SimilarityInterface<T> similarity;
    public int iterations = 5;
    public int partitions = 4;
    public double imbalance = 1.1;

    List<Node<T>> medoids;
    NodePartitioner internal_partitioner;

    public static final String PARTITION_KEY = "BKMP_PARTITION_ID";

    public JavaRDD<Graph<T>> partition(JavaPairRDD<Node<T>, NeighborList> input_graph) {

        input_graph.cache();
        internal_partitioner = new NodePartitioner(partitions);

        // Pick some random initial medoids
        double fraction = 10.0 * partitions / input_graph.count();
        Iterator<Tuple2<Node<T>, NeighborList>> sample_iterator =
                input_graph.sample(false, fraction).collect().iterator();
        medoids = new ArrayList<Node<T>>();
        for (int i = 0; i < partitions; i++) {
            medoids.add(sample_iterator.next()._1);
        }

        // Perform iterations
        for (int iteration = 0; iteration < iterations; iteration++) {

            // Assign each node to a partition id and partition
            JavaPairRDD<Node<T>, NeighborList> partitioned_graph =
                    input_graph.mapPartitionsToPair(
                            new AssignFunction(
                                    medoids,
                                    imbalance,
                                    partitions,
                                    similarity),
                            true).partitionBy(internal_partitioner);

            // Compute new medoids
            computeNewMedoids(partitioned_graph);
        }

        // Perform final partitioning of the input graph
        // Assign each node to a partition id and partition
        JavaPairRDD<Node<T>, NeighborList> partitioned_graph =
                input_graph.mapPartitionsToPair(
                        new AssignFunction(
                                medoids,
                                imbalance,
                                partitions,
                                similarity),
                        true).partitionBy(internal_partitioner);

        // Transform the partitioned PairRDD<Node, Neighborlist>
        // into a distributed graph RDD<Graph>
        JavaRDD<Graph<T>> distributed_graph = partitioned_graph.mapPartitions(
                new NeighborListToGraph(similarity));

        return distributed_graph;
    }

    private static long sum(final long[] values) {
        long agg = 0;
        for (long value : values) {
            agg += value;
        }
        return agg;
    }

    static int argmax(final double[] values) {
        double max_value = -1.0 * Double.MAX_VALUE;
        ArrayList<Integer> ties = new ArrayList<Integer>();

        for (int i = 0; i < values.length; i++) {
            if (values[i] > max_value) {
                max_value = values[i];
                ties = new ArrayList<Integer>();
                ties.add(i);

            } else if (values[i] == max_value) {
                // add a tie
                ties.add(i);
            }
        }

        if (ties.size() == 1) {
            return ties.get(0);
        }

        Random rand = new Random();
        return ties.get(rand.nextInt(ties.size()));
    }

    public void assign(Node<T> node, long[] counts) {
        // Total number of elements
        long n = sum(counts) + 1;
        int partition_constraint = (int) (imbalance * n / partitions);


        double[] similarities = new double[partitions];
        double[] values = new double[partitions];

        // 1. similarities
        for (int center_id = 0; center_id < partitions; center_id++) {
            similarities[center_id] = similarity.similarity(
                    medoids.get(center_id).value,
                    node.value);
        }

        // 2. value to maximize :
        // similarity * (1 - cluster_size / capacity_constraint)
        for (int center_id = 0; center_id < partitions; center_id++) {
            values[center_id] = similarities[center_id]
                    * (1 - counts[center_id] / partition_constraint);
        }

        // 3. choose partition that minimizes compute value
        int partition = argmax(values);
        counts[partition]++;
        node.setAttribute(
                BalancedKMedoidsPartitioner.PARTITION_KEY,
                partition);

    }

    public NodePartitioner getInternalPartitioner() {
        return internal_partitioner;
    }

    public void computeNewMedoids(final JavaRDD<Graph<T>> distributed_graph) {
        medoids = distributed_graph.map(new ComputeMedoids()).collect();
    }

    public void computeNewMedoids(
            final JavaPairRDD<Node<T>, NeighborList> partitioned_graph) {

        JavaRDD<Node<T>> new_medoids = partitioned_graph.mapPartitions(
                new UpdateMedoidsFunction());
        medoids = new_medoids.collect();
    }
}

class UpdateMedoidsFunction<T>
        implements FlatMapFunction<
            Iterator<Tuple2<Node<T>, NeighborList>>,
            Node<T>> {

    public Iterable<Node<T>>
        call(Iterator<Tuple2<Node<T>, NeighborList>> t)
                throws Exception {

        // Build the partition
        Graph partition = new Graph();
        while (t.hasNext()) {
            Tuple2<Node<T>, NeighborList> tuple = t.next();
            partition.put(tuple._1(), tuple._2());
        }

        if (partition.size() == 0) {
            return new ArrayList<Node<T>>();
        }

        // This partition might contain multiple subgraphs => find largest subgraph
        ArrayList<Graph<T>> stronglyConnectedComponents = partition.stronglyConnectedComponents();
        int largest_subgraph_size = 0;
        Graph<T> largest_subgraph = stronglyConnectedComponents.get(0);
        for (Graph<T> subgraph : stronglyConnectedComponents) {
            if (subgraph.size() > largest_subgraph_size) {
                largest_subgraph = subgraph;
                largest_subgraph_size = subgraph.size();
            }
        }

        int largest_distance = Integer.MAX_VALUE;
        Node medoid = (Node) largest_subgraph.getNodes().iterator().next();
        for (Node n : largest_subgraph.getNodes()) {
            //Node n = (Node) o;
            Dijkstra dijkstra = new Dijkstra(largest_subgraph, n);

            int node_largest_distance = dijkstra.getLargestDistance();

            if (node_largest_distance == 0) {
                continue;
            }

            if (node_largest_distance < largest_distance) {
                largest_distance = node_largest_distance;
                medoid = n;
            }
        }
        ArrayList<Node<T>> list = new ArrayList<Node<T>>(1);
        list.add(medoid);

        return list;
    }
}

class AssignFunction<T>
    implements PairFlatMapFunction<
        Iterator<Tuple2<Node<T>, NeighborList>>,
        Node<T>,
        NeighborList> {

    private final List<Node<T>> medoids;
    private final double imbalance;
    private final int partitions;
    private final SimilarityInterface<T> similarity;


    AssignFunction(
            final List<Node<T>> medoids,
            final double imbalance,
            final int partitions,
            final SimilarityInterface<T> similarity) {

        this.medoids = medoids;
        this.imbalance = imbalance;
        this.partitions = partitions;
        this.similarity = similarity;
    }

    public Iterable<Tuple2<Node<T>, NeighborList>> call(
            final Iterator<Tuple2<Node<T>, NeighborList>> iterator) {

        // fetch all tuples in this partition
        // to compute the partition_constraint
        ArrayList<Tuple2<Node<T>, NeighborList>> tuples =
                new ArrayList<Tuple2<Node<T>, NeighborList>>();

        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }

        // this could be estimated with total_n / partitions
        int partition_n = tuples.size();
        double partition_constraint = imbalance * partition_n / partitions;
        int[] partitions_size = new int[partitions];

        for (Tuple2<Node<T>, NeighborList> tuple : tuples) {
            double[] similarities = new double[partitions];
            double[] values = new double[partitions];

            // 1. similarities
            for (int center_id = 0; center_id < partitions; center_id++) {
                similarities[center_id] = similarity.similarity(
                        medoids.get(center_id).value,
                        tuple._1.value);
            }

            // 2. value to maximize =
            // similarity * (1 - cluster_size / capacity_constraint)
            for (int center_id = 0; center_id < partitions; center_id++) {
                values[center_id] =
                        similarities[center_id]
                        * (1 - partitions_size[center_id] / partition_constraint);
            }

            // 3. choose partition that maximizes computed value
            int partition = BalancedKMedoidsPartitioner.argmax(values);
            partitions_size[partition]++;
            tuple._1.setAttribute(
                    BalancedKMedoidsPartitioner.PARTITION_KEY, partition);
        }

        return tuples;
    }
}

class ComputeMedoids<T> implements Function<Graph<T>, Node<T>> {

    public Node<T> call(Graph<T> graph) throws Exception {
        if (graph.size() == 0) {
            return null;
        }

        // This partition might contain multiple subgraphs => find largest subgraph
        ArrayList<Graph<T>> stronglyConnectedComponents = graph.stronglyConnectedComponents();
        int largest_subgraph_size = 0;
        Graph<T> largest_subgraph = stronglyConnectedComponents.get(0);
        for (Graph<T> subgraph : stronglyConnectedComponents) {
            if (subgraph.size() > largest_subgraph_size) {
                largest_subgraph = subgraph;
                largest_subgraph_size = subgraph.size();
            }
        }

        int largest_distance = Integer.MAX_VALUE;
        Node medoid = (Node) largest_subgraph.getNodes().iterator().next();
        for (Node n : largest_subgraph.getNodes()) {
            //Node n = (Node) o;
            Dijkstra dijkstra = new Dijkstra(largest_subgraph, n);

            int node_largest_distance = dijkstra.getLargestDistance();

            if (node_largest_distance == 0) {
                continue;
            }

            if (node_largest_distance < largest_distance) {
                largest_distance = node_largest_distance;
                medoid = n;
            }
        }

        return medoid;
    }
}

class NeighborListToGraph<T>
        implements FlatMapFunction<
            Iterator<Tuple2<Node<T>, NeighborList>>,
            Graph<T>> {

    private final SimilarityInterface<T> similarity;

    public NeighborListToGraph(final SimilarityInterface<T> similarity) {

        this.similarity = similarity;
    }



    public Iterable<Graph<T>> call(
            final Iterator<Tuple2<Node<T>, NeighborList>> iterator)
            throws Exception {

        Graph<T> graph = new Graph<T>();
        while (iterator.hasNext()) {
            Tuple2<Node<T>, NeighborList> next = iterator.next();
            graph.put(next._1, next._2);
        }

        graph.setSimilarity(similarity);
        graph.setK(graph.get(graph.getNodes().iterator().next()).size());

        ArrayList<Graph<T>> list = new ArrayList<Graph<T>>(1);
        list.add(graph);
        return list;

    }
}