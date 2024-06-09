require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "csv"
  gem "pg"
  gem "ruby-progressbar"
end

# Interactive Warning
puts "WARNING: This script will drop the 'firstnames' table and rebuild it. Are you sure you want to proceed? (y/n)"
response = gets.chomp.downcase
abort("Exiting script.") unless response == "y"

# PostgreSQL Connection
conn = PG.connect(dbname: "names2016")

# Drop existing table and rebuild it.
begin
  conn.exec(
    "DROP TABLE IF EXISTS firstnames;
  CREATE TABLE firstnames (
      id SERIAL PRIMARY KEY,
      name VARCHAR(30) NOT NULL,
      gender CHAR(1) CHECK (gender IN ('M', 'F')),
      count INTEGER NOT NULL,
      rank INTEGER,
      year INTEGER
    );"
  )
  conn.exec(
    "GRANT ALL PRIVILEGES ON DATABASE names2016 TO names;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO names;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO names;"
  )
  puts "Privileges granted to 'names' user."
  conn.exec(
    "CREATE INDEX idx_firstnames_year_gender ON firstnames (year, gender);"
  )
rescue PG::Error => e
  puts "Error dropping and rebuilding table: #{e.message}"
end

# Directory containing CSV files
directory_path = "data/firstnames"

# Progress bar for importing CSV files
total_files = Dir[File.join(directory_path, "*.csv")].count
import_progressbar =
  ProgressBar.create(
    title: "Importing CSV Files",
    total: total_files,
    format: "%t: |%B| %p%% %E"
  )

# Hash to store cumulative counts for each name and gender
cumulative_counts = Hash.new { |hash, key| hash[key] = 0 }

# Iterate through files, insert yearly data, and accumulate counts
Dir[File.join(directory_path, "*.csv")].sort.each do |file_path|
  year = File.basename(file_path, ".csv").to_i

  # Hash to track ranks per gender (for yearly data)
  ranks = { "M" => 0, "F" => 0 }

  CSV.foreach(file_path, headers: false) do |row|
    name, gender, count = row
    count = count.to_i

    ranks[gender] += 1 # Increment rank for the gender within this year

    # Insert yearly data
    begin
      conn.exec_params(
        "INSERT INTO firstnames (name, gender, count, rank, year) VALUES ($1, $2, $3, $4, $5)",
        [name, gender, count, ranks[gender], year]
      )
    rescue PG::Error => e
      puts "Error inserting CSV data: #{e.message}"
    end

    # Accumulate counts for each name and gender (for cumulative data)
    cumulative_counts[[name, gender]] += count
  end

  import_progressbar.increment
end

import_progressbar.finish

# Progress bar for calculating and inserting cumulative data
total_names = cumulative_counts.size
cumulative_progressbar =
  ProgressBar.create(
    title: "Calculating & Inserting Cumulative Data",
    total: total_names,
    format: "%t: |%B| %p%% %E"
  )

# Calculate ranks based on cumulative counts
ranks = {}
cumulative_counts
  .group_by { |(name, gender), _| gender }
  .each do |gender, names|
    ranks[gender] = names
      .sort_by { |_, count| -count }
      .each_with_index
      .to_h { |((name, _), count), rank| [name, rank + 1] }
  end

# Insert cumulative data with year 0
cumulative_counts.each do |(name, gender), count|
  begin
    conn.exec_params(
      "INSERT INTO firstnames (name, gender, count, rank, year) VALUES ($1, $2, $3, $4, $5)",
      [name, gender, count, ranks[gender][name], 0]
    )
  rescue PG::Error => e
    puts "Error inserting cumulative data: #{e.message}"
  end

  cumulative_progressbar.increment
end

cumulative_progressbar.finish
conn.close
