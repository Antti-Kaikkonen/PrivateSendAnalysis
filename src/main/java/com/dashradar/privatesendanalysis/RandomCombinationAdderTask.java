
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


public class RandomCombinationAdderTask implements Runnable, Comparable<RandomCombinationAdderTask> {
    
    String txid;
    long priority;
    ThreadPoolExecutor executor;
    int inputCount;
    String neo4jusername;
    String neo4jpassword;
    String evaluatorurl;
    
    private static final long TIME = 10000;
    
    public RandomCombinationAdderTask(String txid, int inputCount, long priority, ThreadPoolExecutor executor, String neo4jusername, String neo4jpassowrd, String evaluatorurl) {
        this.txid = txid;
        this.priority = priority;
        this.executor = executor;
        this.inputCount = inputCount;
        this.neo4jusername = neo4jusername;
        this.neo4jpassword = neo4jpassowrd;
        this.evaluatorurl = evaluatorurl;
    }

    @Override
    public int compareTo(RandomCombinationAdderTask o) {
        return Long.compare(this.priority, o.priority);
    }

    @Override
    public void run() {
        try {
            PsGraph loadFromEvaluator = GraphLoader.loadFromEvaluator(txid, neo4jusername, neo4jpassword, evaluatorurl);
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
                ResultAndCount old = ResultAndCount.loadResultAndCountFromFile(rounds+"/"+txid);
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
                        //result.compute(to, (k, v) -> v == null ? 1 : v+1);
                        combinationResult.compute(to, (k, v) -> v == null ? 1l : 1l);
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
                merged.saveResultAndCountToFile(rounds+"/"+txid);
                if (resultAndCount.notfound == false && resultAndCount.count < mincount) {
                    mincount = resultAndCount.count;
                }
            }
            long newPriority = this.priority+TIME+mincount*10;
            if (newPriority <= 100000) {
                RandomCombinationAdderTask next = new RandomCombinationAdderTask(txid, inputCount, newPriority, executor, neo4jusername, neo4jpassword, evaluatorurl);
                executor.execute(next);
            }
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(RandomCombinationAdderTask.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RandomCombinationAdderTask.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Error ex) {
            
        }
    }
    
}
