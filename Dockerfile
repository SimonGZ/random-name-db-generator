FROM ruby:3.4

# Install system packages needed to build pg gem
RUN apt-get update && apt-get install -y libpq-dev postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Wait script + setup runner
RUN chmod +x wait-for-db.sh

# Run setup script with non-interactive confirmation
CMD ["./wait-for-db.sh"]
