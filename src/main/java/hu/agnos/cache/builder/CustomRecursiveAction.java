package hu.agnos.cache.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ForkJoinTask;

import hu.agnos.cube.dimension.Node;
import hu.agnos.cube.driver.service.Problem;
import hu.agnos.cube.meta.queryDto.CacheKey;
import hu.agnos.cube.meta.resultDto.ResultElement;

public class CustomRecursiveAction extends RecursiveAction {


    private final List<Node> baseVector;
    private final RecursiveProblemPack pack;

    public CustomRecursiveAction(RecursiveProblemPack pack, List<Node> baseVector) {
        this.pack = pack;
        this.baseVector = baseVector;
    }

    @Override
    protected void compute() {
        Problem problem = pack.problemFactory().createProblem(baseVector);
        boolean shouldCalculateCache = (problem.getCachedResult() != null);
        if (!shouldCalculateCache) {
            int affectedRows = (pack.complexity() == 0) ? problem.getNumberOfAffectedIntervals() : problem.getNumberOfAffectedRows();
            shouldCalculateCache = (affectedRows > pack.complexity());
        }
        if (shouldCalculateCache) {
            ResultElement resultElement = problem.compute();
            pack.processedNodes().incrementAndGet();
            CacheKey key = CacheKey.fromNodeList(baseVector);
            if (pack.tmpCache().containsKey(key)) {
                System.err.println("Duplicate key: " + Arrays.toString(resultElement.header()));
            }
            pack.tmpCache().put(key, resultElement.measureValues());
            ForkJoinTask.invokeAll(createSubtasks());
        } else {
            pack.skippedNodes().incrementAndGet();
        }
        pack.recursionDepth().decrementAndGet();
    }

    private List<CustomRecursiveAction> createSubtasks() {
        List<List<Node>> childQueries = getAfterLastNotTopLevelNodeChildQueries();
        pack.recursionDepth().addAndGet(childQueries.size());
        return childQueries.stream().map(childQuery -> new CustomRecursiveAction(pack, childQuery)).toList();
    }

    private List<List<Node>> getAfterLastNotTopLevelNodeChildQueries() {
        int lastNotTopLevelIndex = CustomRecursiveAction.getLastNotTopLevelIndex(baseVector);
        int dimSize = baseVector.size();
        List<List<Node>> childrenList = new ArrayList<>(10);
        for (int dimToDrillIn = lastNotTopLevelIndex; dimToDrillIn < dimSize; dimToDrillIn++) {
            List<Node> childrenInDrillCoordinate = List.of(pack.cube().getDimensions().get(dimToDrillIn).getChildrenOf(baseVector.get(dimToDrillIn)));
            for (Node childInDrillCoordinate : childrenInDrillCoordinate) {
                List<Node> child = new ArrayList<>(dimSize);
                for (int i = 0; i < dimSize; i++) {
                    if (i == dimToDrillIn) {
                        child.add(childInDrillCoordinate);
                    } else {
                        child.add(baseVector.get(i));
                    }
                }
                childrenList.add(child);
            }
        }
        return childrenList;
    }

    private static int getLastNotTopLevelIndex(List<Node> baseVector) {
        int result = 0;
        int size = baseVector.size();
        for (int i = 0; i < size; i++) {
            if (baseVector.get(i).getLevel() != 0) {
                result = i;
            }
        }
        return result;
    }


}