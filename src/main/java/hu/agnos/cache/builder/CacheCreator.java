package hu.agnos.cache.builder;

import hu.agnos.cube.Cube;
import hu.agnos.cube.dimension.Node;
import hu.agnos.cube.driver.service.ProblemFactory;
import hu.agnos.cube.meta.queryDto.CacheKey;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.LoggerFactory;

public class CacheCreator {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CacheCreator.class);

    private final Cube cube;
    private ProblemFactory problemFactory;

    private final AtomicInteger processedNodes = new AtomicInteger(0);
    private final AtomicInteger skippedNodes = new AtomicInteger(0);
    private final AtomicInteger recursionDepth = new AtomicInteger(1);
    private long startTime;
    private Timer logTimer;
    private final ConcurrentMap<CacheKey, double[]> tmpCache = new ConcurrentHashMap<>(10);

    CacheCreator(Cube cube) {
        this.cube = cube;
    }

    public void createCache(int complexity) {
        this.startTime = System.currentTimeMillis();

        startLogger(() -> "Processed: " + processedNodes.get()
                + ", speed: " + Math.round((processedNodes.get()) / ((System.currentTimeMillis() - startTime) / 1000.0)) + " nodes/s"
                + ", recursions: " + recursionDepth.get() + "                            ");
        this.problemFactory = new ProblemFactory(cube);
        List<Node> topNode = cube.getDimensions().stream().map(dimension -> dimension.getNodes()[0][0]).toList();
        addToCacheIfComplexityHigherThan(topNode, complexity);

        long timestamp1 = System.currentTimeMillis();
        stopLogger(() -> "Processed: " + processedNodes.get()
                + ", speed: " + Math.round((processedNodes.get()) / ((System.currentTimeMillis() - startTime) / 1000.0)) + " nodes/s"
                + ", recursions: " + recursionDepth.get() + "                             ");
        System.out.println("Running time: " + (timestamp1-startTime) / 1000 / 60 + " minutes");

    }

    private void addToCacheIfComplexityHigherThan(List<Node> baseVector, int complexity) {
        RecursiveProblemPack pack = new RecursiveProblemPack(cube, tmpCache, problemFactory, complexity, processedNodes, skippedNodes, recursionDepth, startTime);
        CustomRecursiveAction customRecursiveAction = new CustomRecursiveAction(pack, baseVector);
        ForkJoinPool commonPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        commonPool.invoke(customRecursiveAction);
        commonPool.shutdown();
        cube.addAllToCache(tmpCache);
    }

    private void startLogger(Callable<String> logCreator) {
        logTimer = new Timer();
        logTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                long runTime = System.currentTimeMillis() - startTime;
                try {
                    System.out.print(("\r(" + runTime/1000 + " s) " + logCreator.call()).substring(0, 70));
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }, 0, 1000);
    }

    private void stopLogger(Callable<String> finalLogCreator) {
        logTimer.cancel();
        long runTime = System.currentTimeMillis() - startTime;
        try {
            System.out.println(("\r(" + runTime/1000.0 + " s) " + finalLogCreator.call()).substring(0, 70));
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

}
