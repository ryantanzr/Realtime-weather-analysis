#!/bin/bash

# Stop and remove containers, networks, and volumes
docker-compose down

# Remove dangling images
docker image prune -f

# Remove stopped containers
docker container prune -f

# Remove unused volumes
docker volume prune -f

# Remove unused networks
docker network prune -f

echo "Cleanup complete!"