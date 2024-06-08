require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "csv"
  gem "pg"
  gem "ruby-progressbar"
end

# Interactive Warning
puts "WARNING: This script will drop the 'surnames' table and rebuild it. Are you sure you want to proceed? (y/n)"
response = gets.chomp.downcase
abort("Exiting script.") unless response == "y"

# PostgreSQL Connection
conn = PG.connect(dbname: "names2016")

# CSV File Path
csv_file_path = "data/surnames_2010Census.csv"

# Check if CSV file exists
if !File.exist?(csv_file_path)
  puts "Error: CSV file '#{csv_file_path}' not found!"
  abort("Exiting script.")
end

# Read the first line to get column names
begin
  file = File.open(csv_file_path, "r")
  headers = CSV.foreach(csv_file_path).first
  file.close
rescue => e
  puts "Error reading CSV file: #{e.message}"
  abort("Exiting script.")
end

# Count total rows for progress bar
total_rows = CSV.foreach(csv_file_path, headers: true).count

# Insert data from CSV (using headers) and progress bar
puts "Inserting data from CSV..."
import_progressbar =
  ProgressBar.create(
    title: "Importing CSV Data",
    total: total_rows,
    format: "%t: |%B| %p%% %E"
  )

# Define column data types (adjust as needed)
column_types =
  headers.map do |h|
    case h
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
create_table_sql =
  "CREATE TABLE surnames (id SERIAL PRIMARY KEY, #{table_columns});"

# Create table
begin
  conn.exec("DROP TABLE IF EXISTS surnames;")
  conn.exec(create_table_sql)
  puts "Table 'surnames' created successfully."
  conn.exec(
    "GRANT ALL PRIVILEGES ON DATABASE names2016 TO names;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO names;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO names;"
  )
  puts "Privileges granted to 'names' user."
rescue PG::Error => e
  puts "Error dropping and creating table: #{e.message}"
  abort("Exiting script.")
end

# Insert data from CSV (using headers)
puts "Inserting data from CSV..."
CSV.foreach(csv_file_path, headers: true) do |row|
  # Build insert statement with column names and values
  column_names = row.headers.join(", ")
  values = row.map { |_, value| "'#{value.capitalize}'" }.join(", ")
  insert_sql = "INSERT INTO surnames (#{column_names}) VALUES (#{values});"
  conn.exec(insert_sql)
  import_progressbar.increment
end

import_progressbar.finish
puts "Data import completed."

# Close connection
conn.close

puts "Script finished successfully."
