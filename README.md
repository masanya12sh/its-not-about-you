## MatchMaking Test Task Solution

### 1. Overview
This solution implements a basic matchmaking system using two .NET 9 services:
- **MatchMaking.Service.App:** HTTP API for search requests and match retrieval. Also consumes match results from Kafka and stores them in Redis
- **MatchMaking.Worker (x2):** Consumes requests from Kafka, queues users in Redis, forms matches of 3 players, and publishes results back to Kafka

### 2. Prerequisites
- Docker Desktop (or Docker Engine and Docker Compose) installed and running
- Git
- VS

### 3. How to Run the Solution

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/masanya12sh/tresdvcxuytrhgfevsdhjytkugfbdvsta4yrthgf.git
    cd MatchMaking
    ```

2.  **Build and Start the Infrastructure:**
    The `docker-compose.yml` file will launch: 1x Service, 2x Workers, 1x Kafka, 1x Zookeeper, and 1x Redis.
    ```bash
    docker compose up --build -d
    ```

3.  **Check Service Health (Optional):**
    ```bash
    docker compose ps
    ```
    (Wait about 30 seconds for Kafka and all services to fully start.)

### 4. How to Test the Workflow

The API is available at `http://localhost:8080`.

**A. Initiate Search (Rate-Limited):**

```bash
# Request 1
curl -X POST "http://localhost:8080/match/1" -I

# Request 2
curl -X POST "http://localhost:8080/match/2" -I

# Request 3
curl -X POST "http://localhost:8080/match/3" -I

# Request 4
curl -X POST "http://localhost:8080/match/1" -I

**B. Initiate Fetch:**
# Request 5
curl -X GET "http://localhost:8080/match/1" -I

# Request 6
curl -X GET "http://localhost:8080/match/3" -I