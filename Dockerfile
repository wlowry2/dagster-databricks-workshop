# Optimized Dockerfile for Dagster+ Hybrid Deployment
# Uses pre-built wheels (no compilation needed!)
FROM python:3.12-slim

# Set working directory
WORKDIR /opt/dagster/app

# Install only git (removed gcc/g++ - not needed with pre-built wheels!)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy ALL files needed for installation
COPY pyproject.toml ./
COPY src/ src/

# Install the package with all its dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

# Expose port for Dagster gRPC server
EXPOSE 4000

# Default command — refresh Databricks workspace state at startup (uses runtime env vars),
# then start the gRPC server
CMD ["sh", "-c", "dg utils refresh-defs-state --target-path /opt/dagster/app && dagster api grpc -h 0.0.0.0 -p 4000 -m databricks_demo.definitions"]
