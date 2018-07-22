package com.dashradar.privatesendanalysis;

import com.dashradar.dashradarbackend.config.PersistenceContext;
import com.dashradar.dashradarbackend.domain.Transaction;
import com.dashradar.dashradarbackend.repository.BlockRepository;
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
    
    @Autowired
    private BlockRepository blockRepository;
    
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
            
            
            Long fromBlock = blockRepository.findBlockHeightByHash(blockRepository.findBestBlockHash())-10000;
            
            
            while (true) {
                Long toBlock = blockRepository.findBlockHeightByHash(blockRepository.findBestBlockHash());
                if (toBlock > fromBlock) {
                    
                    ArrayList<PrivateSendTransactionDTO> txids = loadPrivateSendTxIds(fromBlock, toBlock);
                    System.out.println("txids.size="+txids.size());
                    Map<PrivateSendTransactionDTO, Long> txidToPriority = txids
                    .parallelStream()
                    .map(pstx -> {
                        Map<String, Object> res = new HashMap<>();
                        res.put("pstx", pstx);
                        try {
                            long priority = Stream.of("2", "3", "4", "5", "6", "7", "8").mapToLong(e -> {
                                try {
                                    ResultAndCount rs = ResultAndCount.loadResultAndCountFromFile(e+"/"+pstx.txid);
                                    if (rs.notfound) {
                                        return Long.MAX_VALUE/2;
                                    } else {
                                        return rs.time+rs.count*100;
                                    }
                                } catch(Exception ex) {
                                    return Long.MAX_VALUE/2;
                                }    
                            }).min().getAsLong();
                            res.put("priority", priority);
                        } catch (Exception ex) {
                            res.put("priority", 0);
                        }
                        return res;
                    })
                    .filter(e -> (long)e.get("priority") >= 0 && (long)e.get("priority") <= 1000000)
                    .collect(Collectors.toMap(toKey -> (PrivateSendTransactionDTO)toKey.get("pstx"), toValue -> (long)toValue.get("priority")));

                    txidToPriority.entrySet()
                            .stream()
                            .map(e -> new RandomCombinationAdderTask(e.getKey().txid, e.getKey().inputCount, e.getValue(), tpe, neo4jusername, neo4jpassword, neo4jevaluatorurl))
                            .sorted()
                            .forEach(task -> {
                                tpe.execute(task); 
                            });
                    
                    fromBlock = toBlock;
                } else {
                    System.out.println("No new blocks");
                }
                System.out.println("TPE:"+tpe.getPoolSize()+", "+ tpe.getActiveCount()+", "+tpe.getQueue().size());
                Thread.sleep(30000);
            }
        };
    }    
    
    
    public ArrayList<PrivateSendTransactionDTO> loadPrivateSendTxIds(long minblock, long maxblock) {
        Session openSession = sessionFactory.openSession();
        HashMap<String, Object> params = new HashMap<>();
        params.put("minblock", minblock);
        params.put("maxblock", maxblock);
        Result privatesendtxids = openSession.query("MATCH (input:TransactionInput)-[:INPUT]->(tx:Transaction {pstype:"+Transaction.PRIVATE_SEND+"})-[:INCLUDED_IN]->(block:Block) "
                + "WHERE block.height >= $minblock AND block.height <= $maxblock "
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
