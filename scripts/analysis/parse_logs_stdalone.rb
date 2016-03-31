#!/usr/bin/env ruby
# Gabriel Tessarolli

Dir.glob('*.txt') do |log_file|

    File.open(log_file, "r") do |infile|
        exec_time = 0
        while (line = infile.gets)
            values = line.strip.split(":")
            case values[0].strip
            when "Job id" 
                jobid = values[1].strip
            when "Query"
                query = values[1].strip
            when "Dbsize"
                dbsize = values[1].strip
            when "Run id"
                run = values[1].strip
            when "Execution time (sec)"
                time = infile.gets.to_s.strip
            when "Size of output"
                size = values[1].split(" ")[0]
            end
            
        end
        puts "#{jobid};#{query};#{run};#{dbsize};#{time};#{size}"
    end
end

