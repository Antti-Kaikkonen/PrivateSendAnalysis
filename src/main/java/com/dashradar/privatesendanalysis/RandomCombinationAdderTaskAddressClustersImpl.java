package com.dashradar.privatesendanalysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import org.jgrapht.graph.DefaultEdge;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

public class RandomCombinationAdderTaskAddressClustersImpl extends AbstractRandomCombinationAdderTask {

    Map<String, String> createDenominationsTransactionToKey = new HashMap<>();
    
    SessionFactory sessionFactory;
    
    public RandomCombinationAdderTaskAddressClustersImpl(String txid, int inputCount, long priority, ThreadPoolExecutor executor, String neo4jusername, String neo4jpassowrd, String evaluatorurl, SessionFactory sessionFactory) {
        super(txid, inputCount, priority, executor, neo4jusername, neo4jpassowrd, evaluatorurl, "psresults/clusters", "psresults/modified/clusters", 30000);
        this.sessionFactory = sessionFactory;
    }

    @Override
    void finish(long newPriority) {
        if (newPriority <= 200000) {
            RandomCombinationAdderTaskAddressClustersImpl next = new RandomCombinationAdderTaskAddressClustersImpl(txid, inputCount, newPriority, executor, neo4jusername, neo4jpassword, evaluatorurl, sessionFactory);
            executor.execute(next);
        }
    }

    @Override
    Consumer<List<List<DefaultEdge>>> getConsumer() {
        return (List<List<DefaultEdge>> combination) -> {
            Map<String, Long> combinationResult = new HashMap<>();
            combination.forEach(e -> {
                DefaultEdge lastEdge = e.get(e.size()-1);
                String to = pi.graph.getEdgeTarget(lastEdge);
                String clusterId;
                if (createDenominationsTransactionToKey.containsKey(to)) {
                    clusterId = createDenominationsTransactionToKey.get(to);
                } else {
                    clusterId = getClusterId2(to).toResultAndCountKey();
                    createDenominationsTransactionToKey.put(to, clusterId);
                }
                //String clusterId = graphToClusterData.get(to);
                //result.compute(to, (k, v) -> v == null ? 1 : v+1);
                combinationResult.compute(clusterId, (k, v) -> v == null ? 1l : 1l);
            });
            resultAndCount.addCombination(combinationResult);   
        };
    }

 
    @Override
    boolean isResultAndCountOutdated(ResultAndCount rs) {
        Map<String, String> oldKeyToNewKey = new HashMap<>();
        boolean isOutdated = rs.result.keySet().stream().anyMatch(key -> {
            String[] split = key.split(",");
            String address = split[0];
            int oldClusterSize = Integer.parseInt(split[1]);
            Cluster cluster = getClusterIdFromAddress(address);
            if (!cluster.clusterAddress.equals(address)) {
                oldKeyToNewKey.put(key, cluster.toResultAndCountKey());
            }
            return cluster.clusterSize != oldClusterSize;
        });
        if (isOutdated) {
            return true;
        } else {
            oldKeyToNewKey.entrySet().forEach(e -> {
                String oldKey = e.getKey();
                String newKeykey = e.getValue();
                Long value = rs.result.get(oldKey);
                if (value == null) throw new RuntimeException("null value while replacing " + oldKey+" with "+newKeykey);
                rs.result.remove(oldKey);
                rs.result.put(newKeykey, value);
            });
            return false;
        }
    }
    
    private int getClusterSize(String clusterAddress) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("address", clusterAddress);
        Result clusterSize = openSession.query("MATCH (:Address {address:$address})-[:INCLUDED_IN]->(c:MultiInputHeuristicCluster) RETURN c.clusterSize as clusterSize;", params);
        for (Map<String, Object> e :clusterSize.queryResults()) {
            return (int) e.get("clusterSize");
        }
        return 1;
    }
    
    private Cluster getClusterIdFromAddress(String address) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("address", address);
        Result txidToCluster = openSession.query(     
        "OPTIONAL MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(:Address {address:$address})\n" +
        "WITH c\n" +
        "OPTIONAL MATCH (c)<-[:INCLUDED_IN]-(firstClusterAddress:Address)\n" +
        "WITH firstClusterAddress.address as clusterAddress, c\n" +
        "LIMIT 1\n" +
        "RETURN coalesce(clusterAddress, $address) as clusterId, coalesce(c.clusterSize, 1) as clusterSize;", params);
        for (Map<String, Object> e :txidToCluster.queryResults()) {
            Cluster c = new Cluster();
            c.clusterSize = (int) e.get("clusterSize");
            c.clusterAddress= (String) e.get("clusterId");
            return c;
        }
        throw new RuntimeException("NO CLUSTERID!");
    }
    
    private Cluster getClusterId2(String txid) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("txid", txid);
        Result txidToCluster = openSession.query(
        "MATCH (address:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:$txid})\n" +
        "WITH address\n" +
        "LIMIT 1\n" +      
        "OPTIONAL MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(address)\n" +
        "WITH address, c\n" +
        "OPTIONAL MATCH (c)<-[:INCLUDED_IN]-(firstClusterAddress:Address)\n" +
        "WITH firstClusterAddress.address as clusterAddress, c, address\n" +
        "LIMIT 1\n" +
        "RETURN coalesce(clusterAddress, address.address) as clusterId, coalesce(c.clusterSize, 1) as clusterSize;", params);
        for (Map<String, Object> e :txidToCluster.queryResults()) {
            Cluster c = new Cluster();
            c.clusterSize = (int) e.get("clusterSize");
            c.clusterAddress= (String) e.get("clusterId");
            return c;
        }
        throw new RuntimeException("NO CLUSTERID!");
    }
    
    private String getClusterId(String txid) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("txid", txid);
        Result txidToCluster = openSession.query(
        "MATCH (a:Address)<-[:ADDRESS]-(:TransactionOutput)-[:SPENT_IN]->(:TransactionInput)-[:INPUT]->(:Transaction {txid:$txid})\n" +
        "WITH collect(a)[0] as address\n" +
        "OPTIONAL MATCH (c:MultiInputHeuristicCluster)<-[:INCLUDED_IN]-(address)\n" +
        "WITH address, c\n" +
        "OPTIONAL MATCH (c)<-[:INCLUDED_IN]-(aa:Address)\n" +
        "WITH min(aa.address) as clusterAddress, c, address\n" +
        "RETURN coalesce(clusterAddress, address.address) as clusterId, coalesce(c.clusterSize, 1) as clusterSize;", params);
        for (Map<String, Object> e :txidToCluster.queryResults()) {
            return (String) e.get("clusterId");
        }
        throw new RuntimeException("NO CLUSTERID!");
    }
    
    private class Cluster {
        int clusterSize;
        String clusterAddress;
        
        public String toResultAndCountKey() {
            return clusterAddress+","+clusterSize;
        }
    }

}
