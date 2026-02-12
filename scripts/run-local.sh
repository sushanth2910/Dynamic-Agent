#!/bin/bash
set -e

# WrenAI Local Stack Runner
# Starts all services using locally built images

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cd "$DOCKER_DIR"

# Check if .env.local exists
if [ ! -f ".env.local" ]; then
    echo -e "${YELLOW}Creating .env.local from template...${NC}"
    cp .env.local.example .env.local
    echo -e "${RED}Please edit docker/.env.local and set your OPENAI_API_KEY${NC}"
    echo -e "${RED}Then run this script again.${NC}"
    exit 1
fi

# Check if config.yaml exists
if [ ! -f "config.yaml" ]; then
    echo -e "${YELLOW}Creating config.yaml from template...${NC}"
    cp config.example.yaml config.yaml
fi

# Create data directory if it doesn't exist
mkdir -p data

ACTION="${1:-up}"

case $ACTION in
    up)
        echo -e "${BLUE}Starting WrenAI local stack...${NC}"
        docker-compose -f docker-compose-local.yaml --env-file .env.local up -d
        echo ""
        echo -e "${GREEN}WrenAI is starting up!${NC}"
        echo -e "Access the UI at: ${BLUE}http://localhost:3000${NC}"
        echo ""
        echo "Check status with: $0 status"
        echo "View logs with: $0 logs"
        echo "Stop with: $0 down"
        ;;
    down)
        echo -e "${YELLOW}Stopping WrenAI local stack...${NC}"
        docker-compose -f docker-compose-local.yaml --env-file .env.local down
        echo -e "${GREEN}All services stopped.${NC}"
        ;;
    restart)
        echo -e "${YELLOW}Restarting WrenAI local stack...${NC}"
        docker-compose -f docker-compose-local.yaml --env-file .env.local restart
        echo -e "${GREEN}All services restarted.${NC}"
        ;;
    status)
        docker-compose -f docker-compose-local.yaml --env-file .env.local ps
        ;;
    logs)
        SERVICE="${2:-}"
        if [ -n "$SERVICE" ]; then
            docker-compose -f docker-compose-local.yaml --env-file .env.local logs -f "$SERVICE"
        else
            docker-compose -f docker-compose-local.yaml --env-file .env.local logs -f
        fi
        ;;
    rebuild)
        echo -e "${YELLOW}Rebuilding and restarting WrenAI...${NC}"
        "$SCRIPT_DIR/build-all-images.sh"
        docker-compose -f docker-compose-local.yaml --env-file .env.local up -d --force-recreate
        echo -e "${GREEN}WrenAI rebuilt and restarted!${NC}"
        ;;
    *)
        echo "Usage: $0 {up|down|restart|status|logs|rebuild}"
        echo ""
        echo "Commands:"
        echo "  up       Start all services (default)"
        echo "  down     Stop all services"
        echo "  restart  Restart all services"
        echo "  status   Show service status"
        echo "  logs     Show logs (optionally: $0 logs <service-name>)"
        echo "  rebuild  Rebuild all images and restart"
        exit 1
        ;;
esac
