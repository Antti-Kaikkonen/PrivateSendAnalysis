package com.dashradar.privatesendanalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.jgrapht.graph.DefaultEdge;

public class PathIteratorImpl extends AbstractPathIterator {

    enum RESULT {
        NOT_FOUND, FOUND, TIMEOUT 
    }
    
    Long stopAt;
    
    public PathIteratorImpl(PsGraph graph) {
        super(graph);
    }

    @Override
    public PathIteratorImpl.RESULT randomCombination(int maxrounds, long timeout) {
        List<List<DefaultEdge>> state = new ArrayList<>();
        List<DefaultEdge> current = new ArrayList<>();
        Set<DefaultEdge> visited = new HashSet<>();
        Map<String, Integer> notFoundVisited = new HashMap<>();
        if (timeout == 0) {
            stopAt = null;
        } else {
            stopAt = System.currentTimeMillis()+timeout;
        }
        try {
            return randomCombinationHelper(state, current, root, visited, maxrounds, notFoundVisited);
        } catch(StackOverflowError ex) {
            System.out.println("StackOverFlowError: txid="+root+", maxrounds="+maxrounds);
            throw ex;
        }
    }
    
    private PathIteratorImpl.RESULT randomCombinationHelper(List<List<DefaultEdge>> state, List<DefaultEdge> current, String from, Set<DefaultEdge> visited, int maxRounds, Map<String, Integer> notFoundVisited) {
        if (stopAt != null && System.currentTimeMillis() > stopAt) return PathIteratorImpl.RESULT.TIMEOUT;
        Set<DefaultEdge> outEdgeSet = graph.outgoingEdgesOf(from);
        if (current.size() > maxRounds+1) return PathIteratorImpl.RESULT.NOT_FOUND;
        if (outEdgeSet.isEmpty()) {//goal reached
            if (pathFilter == null || pathFilter.test(current)) {
                state = new ArrayList<>(state);
                visited = new HashSet<>(visited);
                visited.addAll(current);
                state.add(current);
                if (state.size() == graph.outDegreeOf(root)) {//todo:rething
                    if (acceptedCombinationConsumer != null) {
                        acceptedCombinationConsumer.accept(state);
                    }
                    return PathIteratorImpl.RESULT.FOUND;
                } else {
                    PathIteratorImpl.RESULT res = randomCombinationHelper(state, new ArrayList<>(), root, visited, maxRounds, new HashMap<>());
                    if (res == PathIteratorImpl.RESULT.FOUND) {
                        return PathIteratorImpl.RESULT.FOUND;
                    } else if (res == PathIteratorImpl.RESULT.TIMEOUT) {
                        return PathIteratorImpl.RESULT.TIMEOUT;
                    }        
                }
            }
        } else {
            List<DefaultEdge> outEdgeList = new ArrayList<>(outEdgeSet);
            outEdgeList.removeAll(visited);
            while (!outEdgeList.isEmpty()) {
                DefaultEdge o = outEdgeList.remove(ThreadLocalRandom.current().nextInt(outEdgeList.size()));
                if (notFoundVisited.containsKey(graph.getEdgeTarget(o)) && notFoundVisited.get(graph.getEdgeTarget(o)).equals(current.size())) {
                    continue;
                }
                ArrayList<DefaultEdge> newCurrent = new ArrayList<>();
                newCurrent.addAll(current);
                newCurrent.add(o);
                PathIteratorImpl.RESULT res = randomCombinationHelper(state, newCurrent, graph.getEdgeTarget(o), visited, maxRounds, notFoundVisited);
                if (res == PathIteratorImpl.RESULT.FOUND) {
                    return PathIteratorImpl.RESULT.FOUND;
                } else if (res == PathIteratorImpl.RESULT.TIMEOUT) {
                    return PathIteratorImpl.RESULT.TIMEOUT;
                } else if (res == PathIteratorImpl.RESULT.NOT_FOUND) {
                    notFoundVisited.put(graph.getEdgeTarget(o), current.size());
                }       
                if (current.isEmpty()) break;
            }
        }
        if (stopAt != null && System.currentTimeMillis() > stopAt) {
            return PathIteratorImpl.RESULT.TIMEOUT;
        } else {
            return PathIteratorImpl.RESULT.NOT_FOUND;
        }
    }

    
}
