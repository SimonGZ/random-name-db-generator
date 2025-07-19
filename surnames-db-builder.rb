require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "csv"
  gem "pg"
  gem "ruby-progressbar"
end

# Interactive Warning
if ENV["NONINTERACTIVE"] != "1"
  puts "WARNING: This script will drop the 'firstnames' table and rebuild it. Are you sure you want to proceed? (y/n)"
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
    "GRANT ALL PRIVILEGES ON DATABASE #{ENV.fetch("DB_NAME", "names2016")} TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};"
  )
  puts "Privileges granted to '#{ENV.fetch("DB_USER", "names")}' user."
  conn.exec(
    "CREATE INDEX idx_surnames_prop100k ON surnames (prop100k);
    CREATE INDEX idx_surnames_pctnative ON surnames (pctnative);"
  )
rescue PG::Error => e
  puts "Error dropping and creating table: #{e.message}"
  abort("Exiting script.")
end

# Insert data from CSV (using headers)
puts "Inserting data from CSV..."

# Use COPY for much faster bulk import
conn.copy_data "COPY surnames (#{headers.join(",")}) FROM STDIN CSV" do
  CSV.foreach(csv_file_path, headers: true).with_index do |row, index|
    conn.put_copy_data row.fields.to_csv
    if (index + 1) % 1000 == 0
      puts "Inserted #{index + 1} surname records..."
    end
  end
end

puts "Data import completed."

# Capitalize the names
puts "Capitalizing names..."
conn.exec("UPDATE surnames SET name = INITCAP(name);");
puts "Names capitalized."

# Close connection
conn.close

puts "Script finished successfully."
