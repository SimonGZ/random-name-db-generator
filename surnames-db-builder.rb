require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "csv"
  gem "pg"
end

# Interactive Warning
if ENV["NONINTERACTIVE"] != "1"
  puts "WARNING: This script will drop the 'surnames' table and rebuild it. Are you sure you want to proceed? (y/n)"
  response = gets.chomp.downcase
  abort("Exiting script.") unless response == "y"
else
  puts "Running in non-interactive mode — skipping prompt."
end

# PostgreSQL Connection
conn = PG.connect(
  host:     ENV.fetch("DB_HOST", "localhost"),
  dbname:   ENV.fetch("DB_NAME", "names2016"),
  user:     ENV.fetch("DB_USER", "names"),
  password: ENV["DB_PASSWORD"] # optional; can be nil
)

# CSV File Path
csv_file_path = "data/surnames_2010Census.csv"

# Check if CSV file exists
unless File.exist?(csv_file_path)
  puts "Error: CSV file '#{csv_file_path}' not found!"
  abort("Exiting script.")
end

# Read headers efficiently
headers = nil
begin
  File.open(csv_file_path, "r") do |file|
    headers = CSV.parse_line(file.readline)
  end
rescue => e
  puts "Error reading CSV file: #{e.message}"
  abort("Exiting script.")
end

puts "Found #{headers.length} columns: #{headers.join(', ')}"

# Define column data types (adjust as needed)
column_types = headers.map do |h|
  case h.downcase
  when "name"
    "VARCHAR(30) NOT NULL"
  when "rank", "count"
    "INTEGER NOT NULL"
  else
    "DECIMAL NOT NULL"
  end
end

# Create table statement with dynamic column definitions
table_columns = headers.zip(column_types).map { |h, t| "#{h} #{t}" }.join(", ")
create_table_sql = "CREATE TABLE surnames (id SERIAL PRIMARY KEY, #{table_columns});"

# Create table
begin
  conn.exec("DROP TABLE IF EXISTS surnames;")
  conn.exec(create_table_sql)
  puts "Table 'surnames' created successfully."

  conn.exec(
    "GRANT ALL PRIVILEGES ON DATABASE #{ENV.fetch("DB_NAME", "names2016")} TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};"
  )
  puts "Privileges granted to '#{ENV.fetch("DB_USER", "names")}' user."
rescue PG::Error => e
  puts "Error dropping and creating table: #{e.message}"
  abort("Exiting script.")
end

# Bulk import with batching for better performance
puts "Importing data from CSV..."

batch_size = 5000
batch_buffer = []
total_imported = 0

conn.copy_data "COPY surnames (#{headers.join(",")}) FROM STDIN CSV" do
  CSV.foreach(csv_file_path, headers: true) do |row|
    # Build CSV line manually for better performance than to_csv
    csv_line = row.fields.map { |field|
      field.nil? ? "" : field.to_s.gsub('"', '""')
    }.map { |field|
      field.include?(',') || field.include?('"') || field.include?("\n") ? "\"#{field}\"" : field
    }.join(",") + "\n"

    batch_buffer << csv_line

    if batch_buffer.size >= batch_size
      conn.put_copy_data batch_buffer.join
      total_imported += batch_buffer.size
      batch_buffer.clear
      puts "Imported #{total_imported} surname records..." if total_imported % 10000 == 0
    end
  end

  # Flush remaining batch
  unless batch_buffer.empty?
    conn.put_copy_data batch_buffer.join
    total_imported += batch_buffer.size
  end
end

puts "Data import completed. Total records imported: #{total_imported}"

# Capitalize names and create indexes in a single transaction for better performance
puts "Optimizing data and creating indexes..."
begin
  conn.transaction do |conn_txn|
    # Capitalize the names
    puts "  Capitalizing names..."
    conn_txn.exec("UPDATE surnames SET name = INITCAP(name);")

    # Create indexes after data import (much faster than during import)
    puts "  Creating indexes..."
    conn_txn.exec(
      "CREATE INDEX idx_surnames_prop100k ON surnames (prop100k);
       CREATE INDEX idx_surnames_pctnative ON surnames (pctnative);
       CREATE INDEX idx_surnames_name ON surnames (name);
       CREATE INDEX idx_surnames_rank ON surnames (rank);"
    )
  end
  puts "Optimization completed successfully."
rescue PG::Error => e
  puts "Error during optimization: #{e.message}"
end

# Show summary statistics
begin
  result = conn.exec("SELECT COUNT(*) as total_count, MIN(rank) as min_rank, MAX(rank) as max_rank FROM surnames;")
  stats = result.first
  puts "\nSummary:"
  puts "  Total surnames: #{stats['total_count']}"
  puts "  Rank range: #{stats['min_rank']} to #{stats['max_rank']}"
rescue PG::Error => e
  puts "Error getting summary stats: #{e.message}"
end

# Close connection
conn.close
puts "Script finished successfully."
