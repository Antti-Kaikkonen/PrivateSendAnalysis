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

    Map<String, String> graphToClusterData = new HashMap<>();
    
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
        };
    }
    
    private String getClusterId(String txid) {
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

}
