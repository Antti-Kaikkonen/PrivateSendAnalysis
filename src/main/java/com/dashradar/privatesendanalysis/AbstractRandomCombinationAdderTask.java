package com.dashradar.privatesendanalysis;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgrapht.graph.DefaultEdge;

public abstract class AbstractRandomCombinationAdderTask implements RandomCombinationAdderTask {

    String directory;
    String modifiedDirectory;
    int time;
    
    String txid;
    long priority;
    ThreadPoolExecutor executor;
    int inputCount;
    String neo4jusername;
    String neo4jpassword;
    String evaluatorurl;
    
    PathIteratorImpl pi;
    ResultAndCount resultAndCount;
    
    //private static final long TIME = 30000;
    
    public AbstractRandomCombinationAdderTask(String txid, int inputCount, long priority, ThreadPoolExecutor executor, String neo4jusername, String neo4jpassowrd, String evaluatorurl, String directory, String modifiedDirectory, int time) {
        this.txid = txid;
        this.priority = priority;
        this.executor = executor;
        this.inputCount = inputCount;
        this.neo4jusername = neo4jusername;
        this.neo4jpassword = neo4jpassowrd;
        this.evaluatorurl = evaluatorurl;
        this.directory = directory;
        this.modifiedDirectory = modifiedDirectory;
        this.time = time;
    }

    @Override
    public int compareTo(AbstractRandomCombinationAdderTask o) {
        return Long.compare(this.priority, o.priority);
    }


    abstract Consumer<List<List<DefaultEdge>>> getConsumer();
    

    boolean isResultAndCountOutdated(ResultAndCount rs) {
        return false;
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
                    String path = directory+"/"+i+"/"+txid+".txt";
                    //String path = "psresults/"+directory+"/"+i+"/"+txid+".txt";
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
            this.pi = new PathIteratorImpl(loadFromEvaluator);
            
            long mincount = Long.MAX_VALUE/2;
            for (int rounds = 2; rounds <= 8; rounds++) {
                String filePath = directory+"/"+rounds+"/"+txid+".txt";
                ResultAndCount old = ResultAndCount.loadResultAndCountFromFile(filePath);
                if (isResultAndCountOutdated(old)) {
                    System.out.println(txid+" "+rounds+" outdated!");
                    old = new ResultAndCount();
                }
                if (old.notfound || old.count > 100000) {
                    if (old.count > 100000) System.out.println("OVER 100k");
                    continue;
                }
                System.out.println("executing "+txid+". priority="+priority+", rounds="+rounds);
                long stopAt = System.currentTimeMillis()+time;
                long timeleft = time;
                final int roundsfinal = rounds;
                pi.pathFilter = ((List<DefaultEdge> path) -> path.size() == roundsfinal+1);
                
                this.resultAndCount = new ResultAndCount();
                
                pi.acceptedCombinationConsumer = getConsumer();
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
                resultAndCount.time += time;
                ResultAndCount merged = resultAndCount.combineWith(old);
                File savedFile = merged.saveResultAndCountToFile(filePath);
                try {
                    Path symbolicLinkPath = Paths.get(modifiedDirectory+"/"+rounds+"/"+txid+".txt");
                    Files.createDirectories(symbolicLinkPath.getParent());
                    Files.deleteIfExists(symbolicLinkPath);
                    Files.createSymbolicLink(symbolicLinkPath, symbolicLinkPath.getParent().relativize(Paths.get(filePath)));//For faster rsync
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
                if (resultAndCount.notfound == false && resultAndCount.count < mincount) {
                    mincount = resultAndCount.count;
                }
            }
            long newPriority = this.priority+time+mincount*100;
            finish(newPriority);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(AbstractRandomCombinationAdderTask.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AbstractRandomCombinationAdderTask.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Error ex) {
            
        }
    }

    abstract void finish(long newPriority);
    
}
