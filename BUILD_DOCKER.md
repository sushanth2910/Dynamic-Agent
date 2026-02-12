# Building WrenAI Docker Images

This guide explains how to build all WrenAI service Docker images locally, just like the official release process.

## Architecture Overview

WrenAI consists of the following services:

| Service | Description | Dockerfile Location |
|---------|-------------|---------------------|
| `wren-bootstrap` | Initializes shared config/data volumes | `docker/bootstrap/Dockerfile` |
| `wren-engine` | Core query engine (Java) | `wren-engine/wren-core-legacy/docker/Dockerfile` |
| `ibis-server` | SQL execution layer (Python) | `wren-engine/ibis-server/Dockerfile` |
| `wren-ai-service` | AI/LLM service (Python) | `wren-ai-service/docker/Dockerfile` |
| `wren-ui` | Web frontend (Next.js) | `wren-ui/Dockerfile` |
| `qdrant` | Vector database | Uses official `qdrant/qdrant:v1.11.0` image |

## Prerequisites

- Docker Desktop installed and running
- At least 8GB RAM available for Docker
- ~10GB disk space for all images

## Quick Start - Build All Images

Run the build script from the project root:

```bash
./scripts/build-all-images.sh
```

Or build individually (see below).

---

## Building Individual Services

### 1. Bootstrap Service

```bash
cd docker/bootstrap
docker build -t wrenai/wren-bootstrap:local .
```

### 2. Wren Engine (Java Core)

The wren-engine requires building the Java JAR first:

```bash
cd wren-engine/wren-core-legacy

# Build the JAR with Maven (requires Java 21)
# The -Pexec-jar profile creates the executable JAR
./mvnw clean package -DskipTests -Pexec-jar

# Find the JAR version
WREN_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

# Copy JAR to docker context (JAR is in wren-server submodule)
cp wren-server/target/wren-server-${WREN_VERSION}-executable.jar docker/

# Build Docker image
cd docker
docker build \
  --build-arg WREN_VERSION=${WREN_VERSION} \
  -t wrenai/wren-engine:local .
```

### 3. Ibis Server

The ibis-server depends on wren-core-py (Rust Python bindings):

```bash
cd wren-engine/ibis-server

# Build with multi-stage build (includes wren-core dependencies)
docker build \
  --build-context wren-core-py=../wren-core-py \
  --build-context wren-core=../wren-core \
  --build-context wren-core-base=../wren-core-base \
  -t wrenai/wren-engine-ibis:local .
```

### 4. Wren AI Service

```bash
cd wren-ai-service

# Build from the docker directory context
docker build -f docker/Dockerfile -t wrenai/wren-ai-service:local .
```

### 5. Wren UI

```bash
cd wren-ui
docker build -t wrenai/wren-ui:local .
```

---

## Using Local Images with Docker Compose

Create a `.env.local` file in the `docker/` directory:

```env
COMPOSE_PROJECT_NAME=wrenai-local
PLATFORM=linux/amd64

PROJECT_DIR=.

# Service ports
WREN_ENGINE_PORT=8080
WREN_ENGINE_SQL_PORT=7432
WREN_AI_SERVICE_PORT=5555
WREN_UI_PORT=3000
IBIS_SERVER_PORT=8000
WREN_UI_ENDPOINT=http://wren-ui:3000

# AI service settings
QDRANT_HOST=qdrant
SHOULD_FORCE_DEPLOY=1

# Vendor keys (set your OpenAI key)
OPENAI_API_KEY=your-api-key-here

# LOCAL VERSIONS - point to your built images
WREN_ENGINE_VERSION=local
WREN_AI_SERVICE_VERSION=local
IBIS_SERVER_VERSION=local
WREN_UI_VERSION=local
WREN_BOOTSTRAP_VERSION=local

# Telemetry (optional)
USER_UUID=
POSTHOG_API_KEY=
POSTHOG_HOST=https://app.posthog.com
TELEMETRY_ENABLED=false
GENERATION_MODEL=gpt-4o-mini

# Host port
HOST_PORT=3000
AI_SERVICE_FORWARD_PORT=5555

# Wren UI
EXPERIMENTAL_ENGINE_RUST_VERSION=false
WREN_PRODUCT_VERSION=local

# Local storage
LOCAL_STORAGE=.
```

Then create `docker-compose-local.yaml`:

```yaml
version: "3"

volumes:
  data:

networks:
  wren:
    driver: bridge

services:
  bootstrap:
    image: wrenai/wren-bootstrap:${WREN_BOOTSTRAP_VERSION}
    restart: on-failure
    platform: ${PLATFORM}
    environment:
      DATA_PATH: /app/data
    volumes:
      - data:/app/data
    command: /bin/sh /app/init.sh

  wren-engine:
    image: wrenai/wren-engine:${WREN_ENGINE_VERSION}
    restart: on-failure
    platform: ${PLATFORM}
    expose:
      - ${WREN_ENGINE_PORT}
      - ${WREN_ENGINE_SQL_PORT}
    volumes:
      - data:/usr/src/app/etc
      - ${PROJECT_DIR}/data:/usr/src/app/data
    networks:
      - wren
    depends_on:
      - bootstrap

  ibis-server:
    image: wrenai/wren-engine-ibis:${IBIS_SERVER_VERSION}
    restart: on-failure
    platform: ${PLATFORM}
    expose:
      - ${IBIS_SERVER_PORT}
    environment:
      WREN_ENGINE_ENDPOINT: http://wren-engine:${WREN_ENGINE_PORT}
    volumes:
      - ${LOCAL_STORAGE:-.}:/usr/src/app/data
    networks:
      - wren

  wren-ai-service:
    image: wrenai/wren-ai-service:${WREN_AI_SERVICE_VERSION}
    restart: on-failure
    platform: ${PLATFORM}
    expose:
      - ${WREN_AI_SERVICE_PORT}
    ports:
      - ${AI_SERVICE_FORWARD_PORT}:${WREN_AI_SERVICE_PORT}
    environment:
      PYTHONUNBUFFERED: 1
      CONFIG_PATH: /app/config.yaml
    env_file:
      - ${PROJECT_DIR}/.env.local
    volumes:
      - ${PROJECT_DIR}/config.yaml:/app/config.yaml:ro
      - ${PROJECT_DIR}/data:/app/data:ro
    networks:
      - wren
    depends_on:
      - qdrant

  qdrant:
    image: qdrant/qdrant:v1.11.0
    restart: on-failure
    expose:
      - 6333
      - 6334
    volumes:
      - data:/qdrant/storage
    networks:
      - wren

  wren-ui:
    image: wrenai/wren-ui:${WREN_UI_VERSION}
    restart: on-failure
    platform: ${PLATFORM}
    environment:
      DB_TYPE: sqlite
      SQLITE_FILE: /app/data/db.sqlite3
      WREN_ENGINE_ENDPOINT: http://wren-engine:${WREN_ENGINE_PORT}
      WREN_AI_ENDPOINT: http://wren-ai-service:${WREN_AI_SERVICE_PORT}
      IBIS_SERVER_ENDPOINT: http://ibis-server:${IBIS_SERVER_PORT}
      GENERATION_MODEL: ${GENERATION_MODEL}
      WREN_ENGINE_PORT: ${WREN_ENGINE_PORT}
      WREN_AI_SERVICE_VERSION: ${WREN_AI_SERVICE_VERSION}
      WREN_UI_VERSION: ${WREN_UI_VERSION}
      WREN_ENGINE_VERSION: ${WREN_ENGINE_VERSION}
      USER_UUID: ${USER_UUID}
      POSTHOG_API_KEY: ${POSTHOG_API_KEY}
      POSTHOG_HOST: ${POSTHOG_HOST}
      TELEMETRY_ENABLED: ${TELEMETRY_ENABLED}
      NEXT_PUBLIC_USER_UUID: ${USER_UUID}
      NEXT_PUBLIC_POSTHOG_API_KEY: ${POSTHOG_API_KEY}
      NEXT_PUBLIC_POSTHOG_HOST: ${POSTHOG_HOST}
      NEXT_PUBLIC_TELEMETRY_ENABLED: ${TELEMETRY_ENABLED}
      EXPERIMENTAL_ENGINE_RUST_VERSION: ${EXPERIMENTAL_ENGINE_RUST_VERSION}
      WREN_PRODUCT_VERSION: ${WREN_PRODUCT_VERSION}
    ports:
      - ${HOST_PORT}:3000
    volumes:
      - data:/app/data
    networks:
      - wren
    depends_on:
      - wren-ai-service
      - wren-engine

```

Run with:

```bash
cd docker
docker-compose -f docker-compose-local.yaml --env-file .env.local up -d
```

---

## Troubleshooting

### Build fails for ibis-server
The ibis-server uses `--build-context` which requires Docker BuildKit. Enable it:
```bash
export DOCKER_BUILDKIT=1
```

### wren-engine JAR not found
Make sure you've built the Maven project first and the JAR exists in `target/`.

### Memory issues during build
Increase Docker memory allocation in Docker Desktop settings.

### Platform mismatch (Apple Silicon)
For M1/M2 Macs building for Linux:
```bash
docker build --platform linux/amd64 ...
```
