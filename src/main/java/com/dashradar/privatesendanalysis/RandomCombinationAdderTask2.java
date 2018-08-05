
package com.dashradar.privatesendanalysis;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.Transaction;

//Todo: refactor to remove copy paste from RandomCombinationAdderTask
public class RandomCombinationAdderTask2 implements Runnable, Comparable<RandomCombinationAdderTask2> {
    
    String txid;
    long priority;
    ThreadPoolExecutor executor;
    int inputCount;
    String neo4jusername;
    String neo4jpassword;
    String evaluatorurl;
    SessionFactory sessionFactory;
    
    private static final long TIME = 30000;
    
    public RandomCombinationAdderTask2(String txid, int inputCount, long priority, ThreadPoolExecutor executor, String neo4jusername, String neo4jpassowrd, String evaluatorurl, SessionFactory sessionFactory) {
        this.txid = txid;
        this.priority = priority;
        this.executor = executor;
        this.inputCount = inputCount;
        this.neo4jusername = neo4jusername;
        this.neo4jpassword = neo4jpassowrd;
        this.evaluatorurl = evaluatorurl;
        this.sessionFactory = sessionFactory;
    }

    @Override
    public int compareTo(RandomCombinationAdderTask2 o) {
        return Long.compare(this.priority, o.priority);
    }
    

    @Override
    public void run() {
        try {
            PsGraph loadFromEvaluator = GraphLoader.loadFromEvaluator(txid, neo4jusername, neo4jpassword, evaluatorurl);
            //Map<String, String> graphToClusterData = graphToClusterData(loadFromEvaluator);
            Map<String, String> graphToClusterData = new HashMap<>();
            if (!loadFromEvaluator.rootNode().isPresent()) {
                System.out.println("EMPTY PsGraph ("+txid+")");
                return;
            } else if (loadFromEvaluator.outDegreeOf(loadFromEvaluator.rootNode().get()) != inputCount) {
                for (int i = 2; i <= 8; i++) {
                    String path = "psresults/"+i+"/"+txid+".txt";
                    File f = new File(path);
                    if (f.exists()) {
                        f.delete();
                        System.out.println("deleted "+path);
                    }
                }
                System.out.println("WRONG INPUT COUNT ("+txid+")");
                return;
            }
            //System.out.println("executing "+txid+". timespent="+timespent);
            PathIteratorImpl pi = new PathIteratorImpl(loadFromEvaluator);
            long mincount = Long.MAX_VALUE/2;
            for (int rounds = 2; rounds <= 8; rounds++) {
                ResultAndCount old = ResultAndCount.loadResultAndCountFromFile("clusters/"+rounds+"/"+txid);
                if (old.notfound || old.count > 100000) {
                    if (old.count > 100000) System.out.println("OVER 100k");
                    continue;
                }
                System.out.println("executing "+txid+". priority="+priority+", rounds="+rounds);
                long stopAt = System.currentTimeMillis()+TIME;
                long timeleft = TIME;
                final int roundsfinal = rounds;
                pi.pathFilter = ((List<DefaultEdge> path) -> path.size() == roundsfinal+1);
                
                ResultAndCount resultAndCount = new ResultAndCount();
                pi.acceptedCombinationConsumer = ((List<List<DefaultEdge>> combination) -> {
                    Map<String, Long> combinationResult = new HashMap<>();
                    combination.forEach(e -> {
                        DefaultEdge lastEdge = e.get(e.size()-1);
                        String to = pi.graph.getEdgeTarget(lastEdge);
                        String clusterId;
                        if (graphToClusterData.containsKey(to)) {
                            clusterId = graphToClusterData.get(to);
                        } else {
                            clusterId = getClusterId(to);
                            graphToClusterData.put(to, clusterId);
                        }
                        //String clusterId = graphToClusterData.get(to);
                        //result.compute(to, (k, v) -> v == null ? 1 : v+1);
                        combinationResult.compute(clusterId, (k, v) -> v == null ? 1l : 1l);
                    });
                    resultAndCount.addCombination(combinationResult);               
                });
                PathIteratorImpl.RESULT found = null;
                while (timeleft >= 0) {
                    found = pi.randomCombination(rounds, 1000);
                    timeleft = stopAt - System.currentTimeMillis();
                    if (found == PathIteratorImpl.RESULT.NOT_FOUND) break;
                }
                if (found == PathIteratorImpl.RESULT.NOT_FOUND) {
                    System.out.println(txid+" "+rounds+"-rounds NOT FOUND");
                    resultAndCount.notfound = true;
                } else  if (resultAndCount.count == 0) {
                    System.out.println(txid+" "+rounds+"-rounds TIMEOUT :(");
                } else {
                    System.out.println(txid+" "+rounds+"-rounds FOUND");
                }
                resultAndCount.time += TIME;
                ResultAndCount merged = resultAndCount.combineWith(old);
                merged.saveResultAndCountToFile("clusters/"+rounds+"/"+txid);
                if (resultAndCount.notfound == false && resultAndCount.count < mincount) {
                    mincount = resultAndCount.count;
                }
            }
            long newPriority = this.priority+TIME+mincount*100;
            if (newPriority <= 200000) {
                RandomCombinationAdderTask2 next = new RandomCombinationAdderTask2(txid, inputCount, newPriority, executor, neo4jusername, neo4jpassword, evaluatorurl, sessionFactory);
                executor.execute(next);
            }
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(RandomCombinationAdderTask2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RandomCombinationAdderTask2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Error ex) {
            
        }
    }
    
    public String getClusterId(String txid) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("txid", txid);
        Result txidToCluster = openSession.query(
        "MATCH (a:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:$txid})\n" +
        "WITH $txid as txid, collect(a)[0] as address\n" +
        "OPTIONAL MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(address)\n" +
        "WITH txid, address, c\n" +
        "OPTIONAL MATCH (c)<-[:INCLUDED_IN]-(aa:Address)\n" +
        "WITH txid, min(aa.address) as clusterAddress, c, address\n" +
        "RETURN txid, coalesce(clusterAddress, address.address) as clusterId, coalesce(c.clusterSize, 1) as clusterSize;", params);
        for (Map<String, Object> e :txidToCluster.queryResults()) {
            return (String) e.get("clusterId");
        }
        throw new RuntimeException("NO CLUSTERID!");
    }
    
    public Map<String, String> graphToClusterData(PsGraph graph) {
        List<String> leafNodes = graph.leafNodes();
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("txids", leafNodes);
        Transaction beginTransaction = openSession.beginTransaction();
        Result txidToCluster = openSession.query("CYPHER planner = rule\n" +
        "UNWIND $txids as txid\n" +
        "MATCH (a:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:txid})\n" +
        "WITH txid, collect(a)[0] as address\n" +
        "OPTIONAL MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(address)\n" +
        "WITH txid, address, c\n" +
        "OPTIONAL MATCH (c)<-[:INCLUDED_IN]-(aa:Address)\n" +
        "WITH txid, min(aa.address) as clusterAddress, c, address\n" +
        "RETURN txid, coalesce(clusterAddress, address.address) as clusterId, coalesce(c.clusterSize, 1) as clusterSize;", 
                params, true);
        beginTransaction.close();
        
//        Result txidToCluster = openSession.query("UNWIND $txids as txid\n" +
//        "MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:txid})\n" +
//        "WITH distinct txid, c\n" +
//        "MATCH (c)<-[:INCLUDED_IN]-(a:Address)\n" +
//        "WITH txid, min(a.address) as clusterId, c\n" +
//        "RETURN txid, clusterId, c.clusterSize as clusterSize;", 
//                params);
//        Result txidToCluster = openSession.query("UNWIND $txids as txid\n" +
//            "MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:txid})\n" +
//            "RETURN distinct txid, id(c) as clusterId;", 
//                params);
        
        Map<String, String> result = new HashMap<>();
        for (Map<String, Object> e :txidToCluster.queryResults()) {
            String txid = (String) e.get("txid");
            String clusterid = (String) e.get("clusterId");
            int clusterSize = (int) e.get("clusterSize");//Todo: save this so that calculations can be redone when the cluster expands
            result.put(txid, clusterid);
        }
        
        return result;
    }
    
}
