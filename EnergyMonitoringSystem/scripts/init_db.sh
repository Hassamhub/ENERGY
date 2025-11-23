#!/bin/bash
# Database initialization script for Docker

set -e

echo "Waiting for SQL Server to be ready..."
sleep 30

echo "Initializing PAC3220DB database..."

# Run the complete database schema
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$SA_PASSWORD" -i /tmp/database_init.sql

echo "Database initialization completed successfully!"

# Keep container running for debugging if needed
# tail -f /dev/null