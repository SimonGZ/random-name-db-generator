require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "csv"
  gem "pg"
  gem "ruby-progressbar"
end

# PostgreSQL Connection
conn = PG.connect(dbname: "names2016")

# Directory containing CSV files
directory_path = "data/firstnames" # Replace with the actual path

# Get total number of files for progress bar
total_files =
  (1880..2023).count do |year|
    File.exist?(File.join(directory_path, "#{year}.csv"))
  end

progressbar =
  ProgressBar.create(
    title: "Importing CSV Files",
    total: total_files,
    format: "%t: |%B| %p%% %E"
  ) # Create progress bar

# Iterate through files (assuming they're named 1880.csv, 1881.csv, etc.)
(1880..2023).each do |year|
  file_path = File.join(directory_path, "#{year}.csv")

  # Check if file exists
  if File.exist?(file_path)
    # Hash to track ranks per gender
    ranks = { "M" => 0, "F" => 0 }

    CSV.foreach(file_path, headers: false) do |row|
      name, gender, count = row[0], row[1], row[2].to_i

      ranks[gender] += 1 # Increment rank for the gender

      begin
        conn.exec_params(
          "INSERT INTO firstnames (name, gender, count, rank, year) VALUES ($1, $2, $3, $4, $5)",
          [name, gender, count, ranks[gender], year]
        )
      rescue PG::UniqueViolation
        puts "Skipped duplicate entry: #{name}, #{gender}, #{year}"
      end
    end

    progressbar.increment # Update progress bar after each file
  else
    puts "File not found for year: #{year}"
  end
end

progressbar.finish
# Close Connection
conn.close
