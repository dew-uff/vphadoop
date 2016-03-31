#!/usr/bin/env ruby
# Gabriel Tessarolli

#puts "JOBID;QUERY;RUN;DB SIZE;NFRAGMENTS;NTHREADS;NNODES;PARTITIONING_TIME;SUBQUERY_TIME;PARTIALS_LOADING_TIME;COMPOSITION_TIME;TOTAL_TIME"


def process_one_dir(dir)

    Dir.glob(dir + '/*.txt') do |log_file|

      begin
      
        File.open(log_file, "r") do |infile|
            exec_time = 0
            while (line = infile.gets)
              begin
                next if line.strip.length == 0
                values = line.strip.split(":")
                case values[0].strip
                when "# nodes"
                    nnodes = values[1].strip
                when "# threads"
                    nthreads = values[1].strip
                when "# fragments"
                    nfragments = values[1].strip                
                when "Query"
                    query = values[1].strip
                when "DB size"
                    dbsize = values[1].strip
                when "Run ID"
                    run = values[1].strip
                when "Partitioning time"
                    partitioningtime = values[1].strip.split(" ")[0]
                when /Subquery phase execution time/
                    subquerytime = values[1].strip
                when "Partials loading time"
                    partialsloading = values[1].strip.split(" ")[0]
                when "Composition time"
                    compositiontime = values[1].strip.split(" ")[0]
                when "Total execution time"
                    totaltime = values[1].strip.split(" ")[0]
                when "Shared dir"
                    jobid = values[1].split("_").last.strip
                end
              rescue Exception => e
                puts "Error reading #{line}"
                puts e.message
                puts e.backtrace.inspect
                raise 
              end  
            end
            puts "#{jobid};#{query};#{run};#{dbsize};#{nfragments};#{nthreads};#{nnodes};#{partitioningtime};#{subquerytime};#{partialsloading};#{compositiontime};#{totaltime}"
        end
        
      rescue
        puts "Error reading #{log_file}"
        break
      end
        
    end
end

if ARGV.size > 0
    ARGV.each do |dir|
        process_one_dir(dir)
    end
else
    process_one_dir(".")
end

