package com.dashradar.privatesendanalysis;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.jgrapht.graph.DefaultEdge;


public interface PathIterator {
    
    public void setPsGraph(PsGraph graph);
    
    public void setPathFilter(Predicate<List<DefaultEdge>> pathFilter);
    
    public void setAcceptedCombinationConsumer(Consumer<List<List<DefaultEdge>>> acceptedCombination);
    
    public PathIteratorImpl.RESULT randomCombination(int maxrounds, long timeout);
}
