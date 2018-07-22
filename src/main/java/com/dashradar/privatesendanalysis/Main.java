package com.dashradar.privatesendanalysis;

import com.dashradar.dashradarbackend.config.PersistenceContext;
import com.dashradar.dashradarbackend.domain.Transaction;
import com.dashradar.dashradarbackend.repository.MultiInputHeuristicClusterRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.neo4j.Neo4jDataAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@Component
@Configuration
@EnableAutoConfiguration
@EnableScheduling
@SpringBootApplication(scanBasePackages = "com.dashradar", exclude = {Neo4jDataAutoConfiguration.class})
@Import({PersistenceContext.class})
public class Main {
    
    @Autowired
    private SessionFactory sessionFactory;
    
    @Autowired
    private MultiInputHeuristicClusterRepository multiInputHeuristicClusterRepository;
    
    public static void main(String[] args) throws IOException {
        SpringApplication.run(Main.class, args);
    }
    
    
    //Loaded from application.properties:
    @Value("${neo4jusername}") String neo4jusername;
    @Value("${neo4jpassword}") String neo4jpassword;
    @Value("${neo4jevaluatorurl}") String neo4jevaluatorurl;
    
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        
        return args -> {
            ArrayList<PrivateSendTransactionDTO> txids = loadPrivateSendTxIds(906000, 9900000);
            System.out.println("txids.size:"+txids.size());
            Map<PrivateSendTransactionDTO, Long> txidToPriority = txids
            .parallelStream()
            //.filter(txid -> txid.txid.endsWith("592d1daa5c3cd093aa990ecb67d07eba30634b72f032963c00c7ac2118bc7aca"))
            .map(pstx -> {
                Map<String, Object> res = new HashMap<>();
                res.put("pstx", pstx);
                try {
                    long priority = Stream.of("2", "3", "4", "5", "6", "7", "8").mapToLong(e -> {
                        try {
                            //File f = new File("psresults/"+e+"/"+pstx.txid+".txt");
                            //if (!f.exists()) return Long.MAX_VALUE/2;
                            ResultAndCount rs = ResultAndCount.loadResultAndCountFromFile(e+"/"+pstx.txid);
                            if (rs.notfound) {
                                return Long.MAX_VALUE/2;
                            } else {
                                //if (rs.count == 0 && rs.time < 60000) return 0;
                                return rs.time+rs.count*10;
                            }
                        } catch(Exception ex) {
                            //return 0;
                            return Long.MAX_VALUE/2;//only existing
                        }    
                    }).min().getAsLong();
                    //res.put("mincount", mincount);
                    res.put("priority", priority);
                    //res.put("time", ResultAndCount.loadResultAndCountFromFile("8/"+pstx.txid).time + mincount/10);
                } catch (Exception ex) {
                    res.put("priority", 0);
                    //res.put("mincount", 0);
                }
                return res;
            })
            .filter(e -> (long)e.get("priority") >= 0 && (long)e.get("priority") <= 100000)
            .collect(Collectors.toMap(toKey -> (PrivateSendTransactionDTO)toKey.get("pstx"), toValue -> (long)toValue.get("priority")));
            long min = txidToPriority.entrySet().stream().min((a, b) -> Long.compare(a.getValue(), b.getValue())).get().getValue();
            System.out.println("min priority:"+min);

            long minCount = txidToPriority.entrySet().stream().filter(e -> e.getValue().equals(min)).count();
            System.out.println("mincount="+minCount);
            
            int qInitialSize = 10;
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(16, 16, 1000, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<Runnable>(qInitialSize, new Comparator<Runnable>() {
                @Override
                public int compare(Runnable o1, Runnable o2) {
                    RandomCombinationAdderTask a = (RandomCombinationAdderTask)o1;
                    RandomCombinationAdderTask b = (RandomCombinationAdderTask)o2;
                    return a.compareTo(b);
                }
            }));
            tpe.allowCoreThreadTimeOut(true);
            txidToPriority.entrySet()
                    .stream()
                    //.filter(e -> e.getValue() > 0 && e.getValue() < Long.MAX_VALUE/10)
                    .map(e -> new RandomCombinationAdderTask(e.getKey().txid, e.getKey().inputCount, e.getValue(), tpe, neo4jusername, neo4jpassword, neo4jevaluatorurl))
                    .sorted()
                    .forEach(task -> {
                        tpe.execute(task); 
                    });
            while (true) {
                Thread.sleep(30000);
                System.out.println("TPE:"+tpe.getPoolSize()+", "+ tpe.getActiveCount()+", "+tpe.getQueue().size());
            }
        };
    }    
    
    
    public ArrayList<PrivateSendTransactionDTO> loadPrivateSendTxIds(int minblock, int maxblock) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("minblock", minblock);
        params.put("maxblock", maxblock);
        Result privatesendtxids = openSession.query("MATCH (input:TransactionInput)-[:INPUT]->(tx:Transaction {pstype:"+Transaction.PRIVATE_SEND+"})-[:INCLUDED_IN]->(block:Block) "
                + "WHERE block.height >= $minblock AND block.height < $maxblock "
                + "RETURN tx.txid as txid, count(input) as inputCount, block.height as height;", 
                params);
        ArrayList<PrivateSendTransactionDTO> result = new ArrayList<>();

        for (Map<String, Object> e : privatesendtxids.queryResults()) {
            PrivateSendTransactionDTO pstx = new PrivateSendTransactionDTO();
            pstx.txid = (String) e.get("txid");
            pstx.inputCount = (int) e.get("inputCount");
            pstx.height = (int) e.get("height");
            result.add(pstx);
        };
        return result;
    }
    
    public static class PrivateSendTransactionDTO {
        String txid;
        int inputCount;
        int height;
    }
    
    
}
