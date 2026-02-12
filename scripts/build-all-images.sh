#!/bin/bash
set -e

# WrenAI Docker Image Build Script
# This script builds all WrenAI service images locally

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default tag
TAG="${1:-local}"
PLATFORM="${2:-linux/amd64}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  WrenAI Docker Image Builder${NC}"
echo -e "${BLUE}  Tag: ${TAG}${NC}"
echo -e "${BLUE}  Platform: ${PLATFORM}${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Enable BuildKit
export DOCKER_BUILDKIT=1

build_bootstrap() {
    echo -e "${YELLOW}[1/5] Building wren-bootstrap...${NC}"
    cd "$PROJECT_ROOT/docker/bootstrap"
    docker build \
        --platform "$PLATFORM" \
        -t "wrenai/wren-bootstrap:$TAG" \
        .
    echo -e "${GREEN}✓ wren-bootstrap built successfully${NC}"
}

build_wren_engine() {
    echo -e "${YELLOW}[2/5] Building wren-engine...${NC}"
    cd "$PROJECT_ROOT/wren-engine/wren-core-legacy"
    
    # Check if Maven wrapper exists
    if [ ! -f "./mvnw" ]; then
        echo -e "${RED}Error: Maven wrapper not found. Please run from project root.${NC}"
        exit 1
    fi
    
    echo "  Building Java JAR with Maven (this may take a few minutes)..."
    # The exec-jar profile is required to build the executable JAR
    ./mvnw clean package -DskipTests -Pexec-jar -q
    
    # Get version from pom.xml
    WREN_VERSION=$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)
    echo "  Wren Engine version: $WREN_VERSION"
    
    # The JAR is built in wren-server submodule
    JAR_FILE="wren-server/target/wren-server-${WREN_VERSION}-executable.jar"
    if [ ! -f "$JAR_FILE" ]; then
        echo -e "${RED}Error: JAR file not found at $JAR_FILE${NC}"
        echo -e "${RED}Make sure Maven build completed successfully.${NC}"
        exit 1
    fi
    
    # Copy JAR to docker context
    cp "$JAR_FILE" docker/
    
    # Build Docker image
    cd docker
    docker build \
        --platform "$PLATFORM" \
        --build-arg WREN_VERSION="$WREN_VERSION" \
        -t "wrenai/wren-engine:$TAG" \
        .
    
    # Cleanup
    rm -f "wren-server-${WREN_VERSION}-executable.jar"
    
    echo -e "${GREEN}✓ wren-engine built successfully${NC}"
}

build_ibis_server() {
    echo -e "${YELLOW}[3/5] Building ibis-server...${NC}"
    cd "$PROJECT_ROOT/wren-engine/ibis-server"
    
    docker build \
        --platform "$PLATFORM" \
        --build-context wren-core-py=../wren-core-py \
        --build-context wren-core=../wren-core \
        --build-context wren-core-base=../wren-core-base \
        -t "wrenai/wren-engine-ibis:$TAG" \
        .
    
    echo -e "${GREEN}✓ ibis-server built successfully${NC}"
}

build_ai_service() {
    echo -e "${YELLOW}[4/5] Building wren-ai-service...${NC}"
    cd "$PROJECT_ROOT/wren-ai-service"
    
    docker build \
        --platform "$PLATFORM" \
        -f docker/Dockerfile \
        -t "wrenai/wren-ai-service:$TAG" \
        .
    
    echo -e "${GREEN}✓ wren-ai-service built successfully${NC}"
}

build_ui() {
    echo -e "${YELLOW}[5/5] Building wren-ui...${NC}"
    cd "$PROJECT_ROOT/wren-ui"
    
    docker build \
        --platform "$PLATFORM" \
        -t "wrenai/wren-ui:$TAG" \
        .
    
    echo -e "${GREEN}✓ wren-ui built successfully${NC}"
}

# Parse arguments for selective builds
BUILD_ALL=true
BUILD_BOOTSTRAP=false
BUILD_ENGINE=false
BUILD_IBIS=false
BUILD_AI=false
BUILD_UI=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --bootstrap) BUILD_BOOTSTRAP=true; BUILD_ALL=false; shift ;;
        --engine) BUILD_ENGINE=true; BUILD_ALL=false; shift ;;
        --ibis) BUILD_IBIS=true; BUILD_ALL=false; shift ;;
        --ai) BUILD_AI=true; BUILD_ALL=false; shift ;;
        --ui) BUILD_UI=true; BUILD_ALL=false; shift ;;
        --help)
            echo "Usage: $0 [TAG] [PLATFORM] [OPTIONS]"
            echo ""
            echo "Arguments:"
            echo "  TAG        Docker image tag (default: local)"
            echo "  PLATFORM   Target platform (default: linux/amd64)"
            echo ""
            echo "Options:"
            echo "  --bootstrap  Build only wren-bootstrap"
            echo "  --engine     Build only wren-engine"
            echo "  --ibis       Build only ibis-server"
            echo "  --ai         Build only wren-ai-service"
            echo "  --ui         Build only wren-ui"
            echo "  --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Build all images with tag 'local'"
            echo "  $0 v1.0.0             # Build all images with tag 'v1.0.0'"
            echo "  $0 local --ui         # Build only wren-ui"
            echo "  $0 local --ai --ui    # Build ai-service and ui"
            exit 0
            ;;
        *) shift ;;
    esac
done

# Execute builds
if [ "$BUILD_ALL" = true ]; then
    build_bootstrap
    build_wren_engine
    build_ibis_server
    build_ai_service
    build_ui
else
    [ "$BUILD_BOOTSTRAP" = true ] && build_bootstrap
    [ "$BUILD_ENGINE" = true ] && build_wren_engine
    [ "$BUILD_IBIS" = true ] && build_ibis_server
    [ "$BUILD_AI" = true ] && build_ai_service
    [ "$BUILD_UI" = true ] && build_ui
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  All requested images built!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Built images:"
docker images | grep "wrenai/" | grep "$TAG" || true
echo ""
echo -e "To run: ${BLUE}cd docker && docker-compose -f docker-compose-local.yaml --env-file .env.local up -d${NC}"
