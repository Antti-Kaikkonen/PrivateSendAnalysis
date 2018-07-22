package com.dashradar.privatesendanalysis;

public class RandomCombinationTask implements Runnable {

    PathIteratorImpl pi;
    int rounds;
    long timeout;
    
    public RandomCombinationTask(PsGraph graph, int rounds, long timeout) {
        this.rounds = rounds;
        this.timeout = timeout;
        pi = new PathIteratorImpl(graph);
    }
    
    @Override
    public void run() {
        pi.randomCombination(rounds, timeout);
    }
    
}
