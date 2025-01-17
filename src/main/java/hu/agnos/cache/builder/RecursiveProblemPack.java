package hu.agnos.cache.builder;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import hu.agnos.cube.Cube;
import hu.agnos.cube.driver.service.ProblemFactory;
import hu.agnos.cube.meta.queryDto.CacheKey;

public record RecursiveProblemPack(Cube cube, ConcurrentMap<CacheKey, double[]> tmpCache, ProblemFactory problemFactory, int complexity,
                                   AtomicInteger processedNodes, AtomicInteger skippedNodes, AtomicInteger recursionDepth, long startTime) {
}
