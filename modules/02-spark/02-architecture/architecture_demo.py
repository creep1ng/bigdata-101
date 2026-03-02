"""
Spark Architecture Demo — Partitions, DAG, Stages, Cache.

This script is designed to be explored WITH the Spark UI open.
It pauses at the end so you can inspect everything at http://localhost:4040.

What to look for in the Spark UI:
- Jobs tab:     Each collect()/count() creates a separate job
- Stages tab:   reduceByKey causes a new stage (shuffle boundary)
- Storage tab:  The cached RDD appears here after first action
- Executors tab: Memory and task distribution across workers

Run:
    docker compose exec -it spark-master /opt/spark/bin/spark-submit /app/02-architecture/architecture_demo.py

Note: Use -it flag so the script can pause for input.
"""

from pyspark.sql import SparkSession


def _get_job_ids(sc):
    """Return the set of current job IDs from the status tracker."""
    return set(sc.statusTracker().getJobIdsForGroup())


def demo_partitions(sc):
    """
    Demonstrate how data is split into partitions.

    Partitions are the unit of parallelism in Spark.
    Each partition is processed by one task on one executor.

    Key insight: numSlices controls how many partitions are created.
    More partitions = more parallelism, but also more scheduling overhead.
    """
    print("=" * 50)
    print("1. PARTITIONS")
    print("=" * 50)

    before = _get_job_ids(sc)
    data = list(range(1, 101))  # 100 numbers

    # Create RDD with exactly 4 partitions
    rdd = sc.parallelize(data, numSlices=4)

    # glom() collects elements within each partition into a list
    # This lets us see how Spark distributed the data
    partitions = rdd.glom().collect()

    print(f"  Data: 100 numbers (1 to 100)")
    print(f"  Number of partitions: {rdd.getNumPartitions()}")
    print(f"  Elements per partition: {[len(p) for p in partitions]}")
    print(f"  Partition 0 contains: {partitions[0][:5]}... ({len(partitions[0])} elements)")

    # What happens with a different number of partitions?
    rdd2 = sc.parallelize(data, numSlices=8)
    print(f"\n  Same data, 8 partitions: {[len(p) for p in rdd2.glom().collect()]}")

    jobs = sorted(_get_job_ids(sc) - before)
    print(f"\n  → Check Spark UI: Jobs {jobs} (one per collect call)")


def demo_stages_and_shuffles(sc):
    """
    Demonstrate how Spark splits a job into stages at shuffle boundaries.

    A shuffle happens when data needs to move between partitions
    (e.g., reduceByKey groups all values with the same key together).

    In the Spark UI, each stage appears separately with its own
    "Shuffle Read" and "Shuffle Write" metrics.
    """
    print(f"\n{'=' * 50}")
    print("2. STAGES AND SHUFFLES")
    print("=" * 50)

    before = _get_job_ids(sc)
    data = list(range(1, 101))
    rdd = sc.parallelize(data, numSlices=4)

    # This pipeline has 2 stages:
    #   Stage 1: parallelize → map (no shuffle needed)
    #   Stage 2: reduceByKey (shuffle!) → filter → collect
    result = (
        rdd
        .map(lambda x: (x % 10, x))       # Group by last digit (no shuffle)
        .reduceByKey(lambda a, b: a + b)   # ← SHUFFLE: data moves between partitions
        .filter(lambda x: x[1] > 100)     # Filter after shuffle (no shuffle)
        .collect()                          # Action: triggers execution
    )

    print(f"  Pipeline: parallelize → map → reduceByKey → filter → collect")
    print(f"  Stages: 2 (split at reduceByKey because it causes a shuffle)")
    print(f"\n  Results (sum of numbers by last digit, only > 100):")
    for digit, total in sorted(result):
        print(f"    Digit {digit}: {total}")

    jobs = sorted(_get_job_ids(sc) - before)
    print(f"\n  → Check Spark UI: Job {jobs[0]} has 2 stages (click to see Shuffle Read/Write bytes)")


def demo_cache(sc):
    """
    Demonstrate caching an RDD in memory.

    Without cache: Spark recomputes the RDD from scratch for every action.
    With cache: Spark stores the result in memory after the first action.

    This is critical when you run multiple analyses on the same data
    (like in rdd_uber.py where we run 4 different aggregations).
    """
    print(f"\n{'=' * 50}")
    print("3. CACHE")
    print("=" * 50)

    before = _get_job_ids(sc)
    data = list(range(1, 10001))  # 10,000 numbers

    # Create a pipeline and cache it
    expensive_rdd = (
        sc.parallelize(data, 4)
        .map(lambda x: x * 2)
        .filter(lambda x: x % 3 == 0)
        .cache()  # ← Mark for caching (nothing happens yet)
    )

    # First action: computes AND stores in memory
    count1 = expensive_rdd.count()
    print(f"  First count (computes + caches): {count1}")

    # Second action: reads from memory (no recomputation)
    count2 = expensive_rdd.count()
    print(f"  Second count (from cache): {count2}")

    # We can run more actions without recomputing
    total = expensive_rdd.sum()
    print(f"  Sum (from cache): {int(total)}")

    jobs = sorted(_get_job_ids(sc) - before)
    print(f"\n  → Check Spark UI: Storage tab now shows this cached RDD")
    print(f"  → Jobs {jobs} (count, count from cache, sum from cache)")


def demo_wide_vs_narrow(sc):
    """
    Demonstrate the difference between narrow and wide transformations.

    Narrow (no shuffle): map, filter, flatMap — data stays in its partition
    Wide (shuffle): reduceByKey, groupByKey, join — data moves between partitions

    Understanding this distinction is key to writing efficient Spark code.
    """
    print(f"\n{'=' * 50}")
    print("4. NARROW vs WIDE TRANSFORMATIONS")
    print("=" * 50)

    before = _get_job_ids(sc)
    rdd = sc.parallelize(range(1, 51), numSlices=4)

    # Narrow transformations — no shuffle, 1 stage
    narrow_result = (
        rdd
        .map(lambda x: x * 2)         # Narrow: each element transformed independently
        .filter(lambda x: x > 50)     # Narrow: each element filtered independently
        .count()
    )
    print(f"  Narrow only (map → filter → count): {narrow_result} elements")
    print(f"  → 1 stage, no shuffle")

    # Wide transformation — causes shuffle, 2 stages
    wide_result = (
        rdd
        .map(lambda x: (x % 5, x))    # Narrow
        .reduceByKey(lambda a, b: a + b)  # Wide: data must move between partitions
        .collect()
    )
    print(f"  With wide (map → reduceByKey → collect): {sorted(wide_result)}")
    print(f"  → 2 stages, shuffle between them")

    jobs = sorted(_get_job_ids(sc) - before)
    print(f"\n  → Check Spark UI: Job {jobs[0]} (narrow, 1 stage) and Job {jobs[1]} (wide, 2 stages)")
    print(f"  Rule: minimize wide transformations (shuffles) for better performance")


def main():
    spark = SparkSession.builder.appName("Architecture Demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    demo_partitions(sc)
    demo_stages_and_shuffles(sc)
    demo_cache(sc)
    demo_wide_vs_narrow(sc)

    print(f"\n{'=' * 50}")
    print("Open http://localhost:4040 to explore the Spark UI")
    print("Press Enter to stop...")
    print("=" * 50)

    try:
        input()
    except (KeyboardInterrupt, EOFError):
        pass

    spark.stop()


if __name__ == "__main__":
    main()
