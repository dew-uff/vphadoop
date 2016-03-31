#!/usr/bin/env ruby
# Gabriel Tessarolli

Dir.glob('*.txt') do |log_file|
    if log_file.include? "catalog"
        type = "catalog"
    else
        type = "db"
    end
    File.open(log_file, "r") do |infile|
        while (line = infile.gets)
            values = line.strip.split(":")
            case values[0].strip
            when "# fragments" 
                nfrags = values[1].strip
            when "Query"
                query = values[1].strip
            when "DB size"
                dbsize = values[1].strip
            when "Run ID"
                run = values[1].strip
            when "SVP done! Partitioning time"
                time = values[1].sub!("ms.","").strip
            end unless values[0].nil?
            
        end
        puts "#{type};#{query};#{run};#{dbsize};#{time};#{nfrags}"
    end
end

