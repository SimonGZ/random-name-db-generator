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
  password: ENV["DB_PASSWORD"]
)

# Drop existing table and rebuild it
begin
  conn.exec("DROP TABLE IF EXISTS firstnames;")

  # Create table without ranks initially - we'll calculate them in SQL
  conn.exec(
    "CREATE TABLE firstnames (
      id SERIAL PRIMARY KEY,
      name VARCHAR(30) NOT NULL,
      gender CHAR(1) CHECK (gender IN ('M', 'F')),
      count INTEGER NOT NULL,
      rank INTEGER,
      year INTEGER
    );"
  )

  conn.exec(
    "GRANT ALL PRIVILEGES ON DATABASE #{ENV.fetch("DB_NAME", "names2016")} TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO #{ENV.fetch("DB_USER", "names")};"
  )
  puts "Table created and privileges granted."
rescue PG::Error => e
  puts "Error setting up table: #{e.message}"
  exit 1
end

# Directory containing CSV files
directory_path = "data/firstnames"
csv_files = Dir[File.join(directory_path, "*.csv")].sort
total_files = csv_files.count
puts "Found #{total_files} CSV files to import."

# Import with sequence numbers to preserve CSV row order for ranking
puts "Importing CSV data..."
batch_size = 10000
batch_buffer = []

conn.copy_data "COPY firstnames (name, gender, count, year, rank) FROM STDIN CSV" do
  csv_files.each_with_index do |file_path, file_index|
    year = File.basename(file_path, ".csv").to_i
    puts "Processing #{file_path} (#{file_index + 1}/#{total_files})..."

    # Track ranks per gender (order in CSV determines rank)
    ranks = { "M" => 0, "F" => 0 }

    CSV.foreach(file_path, headers: false) do |row|
      name, gender, count = row
      count = count.to_i
      ranks[gender] += 1

      batch_buffer << "#{name},#{gender},#{count},#{year},#{ranks[gender]}\n"

      if batch_buffer.size >= batch_size
        conn.put_copy_data batch_buffer.join
        batch_buffer.clear
      end
    end
  end

  # Flush remaining batch
  unless batch_buffer.empty?
    conn.put_copy_data batch_buffer.join
  end
end

puts "Yearly data imported with ranks. Now calculating cumulative data..."

# Insert cumulative data (year 0) using PostgreSQL aggregation
puts "Calculating and inserting cumulative data..."
conn.exec(
  "INSERT INTO firstnames (name, gender, count, rank, year)
   SELECT
     name,
     gender,
     SUM(count) as total_count,
     ROW_NUMBER() OVER (PARTITION BY gender ORDER BY SUM(count) DESC) as rank,
     0 as year
   FROM firstnames
   WHERE year > 0
   GROUP BY name, gender;"
)

# Create indexes for performance
puts "Creating indexes..."
begin
  conn.exec(
    "CREATE INDEX idx_firstnames_year_gender ON firstnames (year, gender);
     CREATE INDEX idx_firstnames_name_year ON firstnames (name, year);"
  )
  puts "Indexes created successfully."
rescue PG::Error => e
  puts "Error creating indexes: #{e.message}"
end

# Show summary statistics
result = conn.exec("SELECT year, gender, COUNT(*) as name_count FROM firstnames GROUP BY year, gender ORDER BY year, gender;")
puts "\nSummary:"
result.each do |row|
  year_label = row['year'].to_i == 0 ? "Cumulative" : row['year']
  puts "  #{year_label} #{row['gender']}: #{row['name_count']} names"
end

conn.close
puts "Import completed successfully!"
