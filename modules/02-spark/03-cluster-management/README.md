# Cluster Management

## Your Cluster

The `docker-compose.yml` at the module root defines your cluster:

| Container | Role | UI |
|-----------|------|-----|
| `spark-master` | Coordinates jobs, assigns tasks | [localhost:8080](http://localhost:8080) |
| `spark-worker-1` | Runs executors, processes data | [localhost:8081](http://localhost:8081) |
| `spark-worker-2` | Runs executors, processes data | [localhost:8082](http://localhost:8082) |

## Scaling Workers

Add a third worker:
```bash
docker compose up -d --scale spark-worker-2=1 spark-worker-3
```

Or edit `docker-compose.yml` and add a new service. Then:
```bash
docker compose up -d
```

Check the Master UI — the new worker should appear within seconds.

## Resource Configuration

In `docker-compose.yml`, each worker has:
```yaml
SPARK_WORKER_MEMORY=1G    # RAM available for executors
SPARK_WORKER_CORES=2      # CPU cores available
```

Try changing these values and restarting:
```bash
docker compose down
docker compose up -d
```

## Monitoring with Spark UI

While a job is running, open [localhost:4040](http://localhost:4040):

### Jobs Tab
- See all jobs triggered by actions (`collect`, `count`, etc.)
- Each job has one or more stages

### Stages Tab
- Stages are separated by shuffle boundaries
- Look at "Shuffle Read" and "Shuffle Write" to understand data movement

### Executors Tab
- Memory usage per executor
- Tasks completed, failed, and active
- This is where you see the workers doing actual work

## Exercises

1. **Observe distribution**: Run `rdd_wordcount.py` and check the Executors tab — how many executors ran tasks?

2. **Kill a worker**: Stop one worker while a job runs:
   ```bash
   docker compose stop spark-worker-2
   ```
   Resubmit the job. What happens? Check the Master UI.

3. **Scale up**: Add a third worker and rerun a job. Does it use all three?

4. **Resource limits**: Set `SPARK_WORKER_MEMORY=512M` and run the architecture demo. Check the Executors tab for memory usage.

## Useful Commands

```bash
# Cluster status
docker compose ps

# View logs
docker compose logs spark-master
docker compose logs spark-worker-1

# Restart cluster
docker compose restart

# Full reset
docker compose down
docker compose up -d
```
