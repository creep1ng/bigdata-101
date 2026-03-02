# Getting Started with Spark

## Prerequisites

- Docker and Docker Compose
- Git (to clone the repo)

That's it. No need to install Java or PySpark locally — everything runs inside Docker.

## Step 1: Start the Cluster

```bash
cd modules/02-spark
docker compose up -d
```

Verify everything is running:
```bash
docker compose ps
```

You should see 3 containers: `spark-master`, `spark-worker-1`, `spark-worker-2`.

## Step 2: Open the Spark Master UI

Open [http://localhost:8080](http://localhost:8080) in your browser. You should see:
- 2 workers registered
- Available memory and cores

## Step 3: Run Your First Job

```bash
docker compose exec spark-master spark-submit /app/01-rdd-basics/rdd_wordcount.py
```

## How It Works

```
Your machine                    Docker containers
┌──────────────┐    volume     ┌──────────────┐
│  02-spark/   │ ──────────▶  │   /app/       │
│  (your code) │    mount     │  (same files) │
└──────────────┘              └──────────────┘
```

The `docker-compose.yml` mounts the entire `02-spark/` folder as `/app` inside the containers. When you edit a script locally, it's immediately available in the cluster.

## Running Scripts

All scripts in this module run the same way:

```bash
# Pattern
docker compose exec spark-master spark-submit /app/<section>/<script>.py

# Examples
docker compose exec spark-master spark-submit /app/01-rdd-basics/rdd_wordcount.py
docker compose exec spark-master spark-submit /app/01-rdd-basics/rdd_temperature.py
docker compose exec spark-master spark-submit /app/02-architecture/architecture_demo.py
```

## Spark UI URLs

| URL | Description |
|-----|-------------|
| [localhost:8080](http://localhost:8080) | Master UI — workers, running apps |
| [localhost:8081](http://localhost:8081) | Worker 1 UI |
| [localhost:8082](http://localhost:8082) | Worker 2 UI |
| [localhost:4040](http://localhost:4040) | Application UI (while a job runs) |

## Stopping the Cluster

```bash
docker compose down
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `docker: command not found` | Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) |
| Workers not appearing in UI | Wait 10-15 seconds, then refresh |
| Port 8080 already in use | Change the port in `docker-compose.yml` |
| `Connection refused` on submit | Ensure cluster is running: `docker compose ps` |
