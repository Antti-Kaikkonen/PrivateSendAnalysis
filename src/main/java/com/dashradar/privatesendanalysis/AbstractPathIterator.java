package com.dashradar.privatesendanalysis;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.jgrapht.graph.DefaultEdge;

public abstract class AbstractPathIterator implements PathIterator {
    
    Consumer<List<List<DefaultEdge>>> acceptedCombinationConsumer;
    Predicate<List<DefaultEdge>> pathFilter;
    
    PsGraph graph;
    String root;
    
    public AbstractPathIterator(PsGraph graph) {
        this.graph = graph;
        this.root = graph.rootNode().get();
    }
    
    @Override
    public void setAcceptedCombinationConsumer(Consumer<List<List<DefaultEdge>>> acceptedCombinationConsumer) {
        this.acceptedCombinationConsumer = acceptedCombinationConsumer;
    }

    @Override
    public void setPathFilter(Predicate<List<DefaultEdge>> pathFilter) {
        this.pathFilter = pathFilter;
    }

    @Override
    public void setPsGraph(PsGraph graph) {
        this.graph = graph;
    }
}
