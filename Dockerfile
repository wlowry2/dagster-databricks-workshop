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

# Fetch Databricks workspace state at build time (requires DATABRICKS_HOST and DATABRICKS_CONNECTION_TOKEN build args)
ARG DATABRICKS_HOST
ARG DATABRICKS_CONNECTION_TOKEN
RUN if [ -n "$DATABRICKS_HOST" ] && [ -n "$DATABRICKS_CONNECTION_TOKEN" ]; then \
      DATABRICKS_HOST=$DATABRICKS_HOST DATABRICKS_CONNECTION_TOKEN=$DATABRICKS_CONNECTION_TOKEN \
      dg utils refresh-defs-state --project-dir /opt/dagster/app; \
    fi

# Default command (will be overridden by Dagster+ agent)
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "databricks_demo.definitions"]
