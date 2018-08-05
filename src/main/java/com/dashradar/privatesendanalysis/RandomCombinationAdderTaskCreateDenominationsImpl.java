package com.dashradar.privatesendanalysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import org.jgrapht.graph.DefaultEdge;

public class RandomCombinationAdderTaskCreateDenominationsImpl extends AbstractRandomCombinationAdderTask {

    public RandomCombinationAdderTaskCreateDenominationsImpl(String txid, int inputCount, long priority, ThreadPoolExecutor executor, String neo4jusername, String neo4jpassowrd, String evaluatorurl) {
        super(txid, inputCount, priority, executor, neo4jusername, neo4jpassowrd, evaluatorurl, "psresults", "psresults/modified", 30000);
    }

    @Override
    void finish(long newPriority) {
        if (newPriority <= 200000) {
            RandomCombinationAdderTaskCreateDenominationsImpl next = new RandomCombinationAdderTaskCreateDenominationsImpl(txid, inputCount, newPriority, executor, neo4jusername, neo4jpassword, evaluatorurl);
            executor.execute(next);
        }
    }

    @Override
    Consumer<List<List<DefaultEdge>>> getConsumer() {
        return ((List<List<DefaultEdge>> combination) -> {
            Map<String, Long> combinationResult = new HashMap<>();
            combination.forEach(e -> {
                DefaultEdge lastEdge = e.get(e.size()-1);
                String to = pi.graph.getEdgeTarget(lastEdge);
                //result.compute(to, (k, v) -> v == null ? 1 : v+1);
                combinationResult.compute(to, (k, v) -> v == null ? 1l : 1l);
            });
            resultAndCount.addCombination(combinationResult);               
        });
    }
    
}
