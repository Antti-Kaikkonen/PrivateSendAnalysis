package com.dashradar.privatesendanalysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

public class GraphLoader {
   
    private static PoolingHttpClientConnectionManager poolingConnManager = new PoolingHttpClientConnectionManager();
    
    private static CloseableHttpResponse fromEvaluator(String txid, int maxrounds, String neo4jusername, String neo4jpassword, String evaluatorURL) throws IOException {
        poolingConnManager.setMaxTotal(50);
        poolingConnManager.setDefaultMaxPerRoute(16);
        String encoding = Base64.getEncoder().encodeToString((neo4jusername+":"+neo4jpassword).getBytes());
        
        CloseableHttpClient client
        = HttpClients.custom().setConnectionManager(poolingConnManager)
        .build();
        
        HttpPost httppost = new HttpPost(evaluatorURL);
        httppost.setHeader("Authorization", "Basic " + encoding);
        httppost.setEntity(new StringEntity("{\"txid\":\""+txid+"\", \"maxRounds\":\""+maxrounds+"\"}"));
        return client.execute(httppost);
    }
    
    
    public static void evaluatorToFile(String txid, int maxrounds, String neo4jusername, String neo4jpassword, String evaluatorURL) throws FileNotFoundException, IOException {
        try (CloseableHttpResponse response = fromEvaluator(txid, maxrounds, neo4jusername, neo4jpassword, evaluatorURL)) {
            HttpEntity entity = response.getEntity();
            String fileName = txid+".csv";
            try (
                    OutputStream os = new FileOutputStream("psgraphs/"+fileName); 
                    InputStream is = entity.getContent();
                ) 
            {
                IOUtils.copy(is, os);
            }
        }
    }
    
    
    public static List<PsGraph> segmentGraph(PsGraph graph) {
        List<PsGraph> result = new ArrayList<>();
        String root = graph.rootNode().get();
        Set<DefaultEdge> inputs = graph.outgoingEdgesOf(root);
        Set<String> firstRoundTxIds = inputs.stream().map(input -> graph.getEdgeTarget(input)).collect(Collectors.toSet());
        firstRoundTxIdLoop : for (String firstRoundTxId : firstRoundTxIds) {
            PsGraph segment = new PsGraph();
            org.jgrapht.traverse.DepthFirstIterator<String, DefaultEdge> iter = new DepthFirstIterator<>(graph, firstRoundTxId);
            String previous = firstRoundTxId;
            while (iter.hasNext()) {
                String next = iter.next();
                Set<DefaultEdge> edges = iter.getGraph().outgoingEdgesOf(next);
                for (DefaultEdge edge : edges) {
                    String source = iter.getGraph().getEdgeSource(edge);
                    String target = iter.getGraph().getEdgeTarget(edge);
                    segment.addVertex(source);
                    segment.addVertex(target);
                    boolean added = segment.addEdge(source, target, edge);
                }
                if (graph.outDegreeOf(next) != 0) {
                    Optional<PsGraph> overlappingGraph = result.stream().filter(g -> g.vertexSet().contains(next)).findAny();
                    if (overlappingGraph.isPresent()) {
                        System.out.println("overlapping found at "+next+", indegree:"+graph.inDegreeOf(next));
                        Graphs.addGraph(overlappingGraph.get(), segment);
                        continue firstRoundTxIdLoop;
                    } else {
                        System.out.println("no overlap");
                    }
                }
                previous = next;
                
            }
            result.add(segment);
        }
        return result;
    }
    
    public static PsGraph loadFromEvaluator(String txid, String neo4jusername, String neo4jpassword, String evaluatorURL) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        try (CloseableHttpResponse response = fromEvaluator(txid, 8, neo4jusername, neo4jpassword, evaluatorURL)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Wrong status code ("+response.getStatusLine()+")"+" txid="+txid);
            }
            HttpEntity entity = response.getEntity();
            PsGraph result = new PsGraph();

            try (Scanner scanner = new Scanner(entity.getContent())) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    String[] fields = line.split(",");
                    String fromTxid = fields[0].replace("\"", "");
                    String toTxid = fields[1].replace("\"", "");
                    String connectionsStr = fields[2].replace("\"", "");
                    int connections = Integer.parseInt(connectionsStr);

                    result.addVertex(fromTxid);
                    result.addVertex(toTxid);
                    for (int i = 0; i < connections; i++) {
                        DefaultEdge edge = result.addEdge(fromTxid, toTxid);
                    }
                }
            }
            EntityUtils.consume(entity);
            return result;
        }
    }
    
    
    public static PsGraph loadFromFile(String filename) throws FileNotFoundException {
        PsGraph result = new PsGraph();
        
        File f = new File(filename);
        Scanner scanner = new Scanner(f);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] fields = line.split(",");
            String fromTxid = fields[0].replace("\"", "");
            String toTxid = fields[1].replace("\"", "");
            String connectionsStr = fields[2].replace("\"", "");
            int connections = Integer.parseInt(connectionsStr);

            result.addVertex(fromTxid);
            result.addVertex(toTxid);
            for (int i = 0; i < connections; i++) {
                DefaultEdge edge = result.addEdge(fromTxid, toTxid);
            }
        }
        return result;
    }
}
