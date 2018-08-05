package com.dashradar.privatesendanalysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

public class ResultAndCount {
   
    
    Map<String, Long> result;
    long count;
    long time;
    boolean notfound = false;
    
    public ResultAndCount(Map<String, Long> result, long count, long time, boolean notfound) {
        this.result = result;
        this.count = count;
        this.time = time;
        this.notfound = notfound;
    }
    
    public ResultAndCount() {
        this.result = new HashMap<>();
        this.count = 0;
        this.time = 0;
        this.notfound = false;
    }
    
    public ResultAndCount combineWith(ResultAndCount rs2) {
        Map<String, Long> res = new HashMap<>(this.result);
        Set<String> allKeys = new HashSet<>(this.result.keySet());
        allKeys.addAll(rs2.result.keySet());
        for (String targetNode: allKeys) {
            Long bCount = this.result.getOrDefault(targetNode, 0l);
            Long aCount = rs2.result.getOrDefault(targetNode, 0l);
            res.put(targetNode, aCount+bCount);
        }
        ResultAndCount result = new ResultAndCount();
        result.result = res;
        result.count = this.count + rs2.count;
        result.time = this.time + rs2.time;
        result.notfound = this.notfound || rs2.notfound;
        return result;
    }
    
    
    public void addCombination(Map<String, Long> add) {
        for (String targetNode: add.keySet()) {
            Long bCount = this.result.getOrDefault(targetNode, 0l);
            Long aCount = add.getOrDefault(targetNode, 0l);
            this.result.put(targetNode, aCount+bCount);
        }
        if (!add.isEmpty()) {
            this.count++;
        }
    }
    
    
    
    public static ResultAndCount loadResultAndCountFromFile(String filePath) throws FileNotFoundException {
        ResultAndCount rs = new ResultAndCount();
        try (Scanner scanner = new Scanner(new File(filePath))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] components = line.split("=");
                if (components[0].equals("count")) {
                    rs.count = Math.max(0, Long.parseLong(components[1]));
                } else if (components[0].equals("time")) {
                    rs.time = Math.max(0, Long.parseLong(components[1]));
                } else if (components[0].equals("notfound")) {
                    rs.notfound = Boolean.parseBoolean(components[1]);
                } else {
                    rs.result.put(components[0], Math.max(0, Long.parseLong(components[1])));
                }
            }
        } catch(Exception ex) {
            //System.out.println(ex);
        }
        return rs;
    }
    
    public File saveResultAndCountToFile(String filePath) throws FileNotFoundException, IOException {
        //File f = new File("psresults/"+txid+".txt");
        File f = new File(filePath);
        f.getParentFile().mkdirs();
        PrintWriter pw = new PrintWriter(f);
        pw.println("count="+this.count);
        pw.println("time="+this.time);
        pw.println("notfound="+this.notfound);
        this.result.entrySet().forEach(e -> {
            pw.println(e.getKey()+"="+e.getValue());
        });
        pw.close();
        return f;
//        try {
//            Path symbolicLinkPath = Paths.get("psresults/modified/"+txid+".txt");
//            Files.createDirectories(symbolicLinkPath.getParent());
//            Files.deleteIfExists(symbolicLinkPath);
//            Files.createSymbolicLink(symbolicLinkPath, symbolicLinkPath.getParent().relativize(Paths.get("psresults/"+txid+".txt")));//For faster rsync
//        } catch(Exception ex) {
//            ex.printStackTrace();
//        }
    }
    
    public ResultAndCount mergeResultAndCountToFile(String filePath) throws IOException {
        ResultAndCount old = loadResultAndCountFromFile(filePath);
        ResultAndCount merged = this.combineWith(old);
        merged.saveResultAndCountToFile(filePath);
        return merged;
    }
    
    
    
    public Map<String, Double> percentages() {
        return this.result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                  e -> 1.0*e.getValue()/this.count));
    }
    
    
}
