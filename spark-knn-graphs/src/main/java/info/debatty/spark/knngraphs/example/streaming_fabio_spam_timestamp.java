package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import info.debatty.spark.knngraphs.builder.Online;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


/**
 *
 * @author Fabio
 *
 * this class should be run together with a tcp streaming event source
 * it modify the graph (adding and deleting the nodes at the end of the batch)
 * but the deletion is now handled in a time-wise fashion
 * with a sliding window of a certain period - all the nodes are collected in a local stack fifo
 * from this point of view. do we really need to store the timestamp information in the graph?

 */
//
    // it modify the graph (adding and deleting the nodes at the end of the batch) but the deleting is now

public class streaming_fabio_spam_timestamp {

    /**
     * @param args the command line arguments
     * @throws IOException
     */

    // Number of nodes in the initial graph
    static final int N = 200;

    // Number of nodes to add to the graph
    static final int N_TEST = 200;
    static final int PARTITIONS = 4;
    static final int K = 10;
    static final double SUCCESS_RATIO = 0.8;
    private static final String HOST = "localhost";
    private static final int PORT = 9999;
    private static long period = 100000;
    private static int batch=1;


    public static void main(String[] args) throws IOException, Exception {
        if (args.length != 1) {
            System.out.println(
                    "Usage: spark-submit --class " +
                            Search.class.getCanonicalName() + " " +
                            "<dataset>");
        }

        String file = args[0];

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        int sequence_number = 0;
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            if (sequence_number == N) {
                break;
            }
            Node<String> node = new Node<String>(String.valueOf(data.size()), s);
            node.setAttribute(Online.NODE_SEQUENCE_KEY, sequence_number);
            java.util.Date date = new java.util.Date();
            node.setAttribute("TIMESTAMP", date.getTime());
            data.add(node);
            sequence_number++;
        }


        SparkConf config = new SparkConf().setAppName("spam");
        config.set("spark.streaming.backpressure.enabled", "true");
        config.set("spark.streaming.stopGracefullyOnShutdown", "true");
        JavaSparkContext sc = new JavaSparkContext(config);
        sc.setLogLevel("OFF");

      

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        //Define similarity
        SimilarityInterface<String> similarity =
                new SimilarityInterface<String>() {

                    public double similarity(String value1, String value2) {
                        JaroWinkler jw = new JaroWinkler();
                        return jw.similarity(value1, value2);
                    }
                };

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<String>, NeighborList> graph =
                brute.computeGraph(nodes);
        System.out.println(graph.first());

        System.out.println("Prepare the graph for online processing");
        final Online<String> online_graph =
                new Online<String>(K, similarity, sc, graph, PARTITIONS);



        final Accumulator<Integer> accum_seq = sc.accumulator(sequence_number);
        final int sequence_number_final = sequence_number;


        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(batch));


        List<String> data2 = new ArrayList<String>();
        JavaRDD<String> data2RDD = sc.parallelize(data2);
        LinkedList<Node<String>> nodes_list= new LinkedList<Node<String>>(); //structure to delete nodes (stack FIFO)


        // Checkpointing must be enabled to use the updateStateByKey function.
       // ssc.checkpoint("./checkpoints");


        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOST, PORT);
        
        //input is in format "sent:spam_mail". Keep the last part
        JavaDStream<String> events = lines.map(
                new Function<String, String>() {
                    @Override
                    public String call(String rawEvent) throws Exception {
                        String[] strings = rawEvent.split(":");
                        return strings[1];
                    }
                }
        );

        JavaDStream<String> events_window = events.window(
                Durations.seconds(batch), Durations.seconds(batch));//a set of operations that happens just after
                                                                    // each batch (with this conf)
        events_window.count().print(); //arrivals
        //events_window.print();
        List<String> data3 = new ArrayList<String>();


        //at each end of the batch we add the new nodes delete the older expired nodes
        events_window.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> rdd) throws Exception {
// TODO Auto-generated method stub
                if (rdd != null) {
                    List<String> result = rdd.collect(); // =(
                    data3.addAll(result);
                    for (String s : result) {
                        java.util.Date date = new java.util.Date();
                        Node<String> node = new Node<String>(String.valueOf(accum_seq.value()), s);
                        node.setAttribute(Online.NODE_SEQUENCE_KEY, accum_seq.value());
                        node.setAttribute("TIMESTAMP", date.getTime()); //add also the timestamp!
                        online_graph.fastAdd(node);
                      //  System.out.println("just added a node with ts" + node.getAttribute("TIMESTAMP"));
                        nodes_list.add(node); //nodes in a stack
                        accum_seq.add(1);


                    }
                    boolean flag = TRUE;
                    java.util.Date date = new java.util.Date();
                    long now = date.getTime();
                    //this cycle deletes the nodes from the stack and from the graph
                    //through the info of the stack.
                    // if the first (older) nodes of the stack is expired, delete it and pass to the the second
                    while (flag) {
                        Node<String> node = nodes_list.getFirst();
                        if ((long) node.getAttribute("TIMESTAMP")<(now-period)) {
                            nodes_list.removeFirst();
                            online_graph.fastRemove(node);
                        }
                        else flag=FALSE;
                    }
                    System.out.println("node_list size is: "+nodes_list.size());
                    //for (Node n : nodes_list) System.out.println("it contains: "+n.getAttribute("TIMESTAMP"));
                    Graph<String> local_approximate_graph =
                            list2graph(online_graph.getGraph().collect());
                    System.out.println("graph size is: "+local_approximate_graph.size());
                    List<Node<String>> data3 = new ArrayList<Node<String>>();
                    for (Node<String> n: local_approximate_graph.getNodes()) {data3.add(n);}
                    Graph<String> local_exact_graph =
                            list2graph(brute.computeGraph(sc.parallelize(data3)).collect());

                    int correct = 0;
                    for (Node<String> node : local_exact_graph.getNodes()) {
                        try {
                            correct += local_exact_graph.get(node).countCommons(
                                    local_approximate_graph.get(node));
                        } catch (Exception ex) {
                            System.out.println("Null neighborlist!");
                        }
                    }
                    System.out.println("Found " + correct + " correct edges");
                    double ratio = 1.0 * correct / (data3.size() * K);
                    System.out.println("= " + ratio * 100 + " %");

                }
                return null;
            }
        });






        ssc.start();
        ssc.awaitTermination(400000);

        Graph<String> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());

        //sc.close();
        //Thread.sleep(3000);


    }

    private static Graph<String> list2graph(
            final List<Tuple2<Node<String>, NeighborList>> list) {

        Graph<String> graph = new Graph<String>();
        for (Tuple2<Node<String>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }

}
