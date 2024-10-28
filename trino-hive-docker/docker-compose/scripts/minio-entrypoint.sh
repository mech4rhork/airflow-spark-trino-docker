#!/bin/sh

# Wait for MinIO to start
until $(curl --output /dev/null --silent --head --fail http://minio:9000); do
    printf '.'
    sleep 10
done

# Configure MinIO Client (mc)
mc alias set myminio http://minio:9000 minio minio123

# Create the bucket
mc mb myminio/main-bucket

# Start MinIO server
server --console-address ":9001" /data
