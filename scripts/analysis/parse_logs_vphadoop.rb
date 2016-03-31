#!/usr/bin/env ruby
# Gabriel Tessarolli

def parse_log_header(infile)
    while (line = infile.gets)
        break if (line.strip == "------------------------------------------")

        values = line.strip.split(":")
        case values[0].strip
        when "# nodes"
            nnodes = values[1].strip 
        when "# tasks per node" 
            ntasks = values[1].strip
        when "Query"
            query = values[1].strip
        when "DB size"
            dbsize = values[1].strip
        when "Run ID"
            run = values[1].strip
        when "Num splits"
            nsplits = values[1].strip
        when "Num records"
            nrecords = values[1].strip
        when "Execution time (sec)"
            time = infile.gets.to_s.strip
        when "Size of output"
            size = values[1].split(" ")[0]
        when "JOB ID"
            jobid = values[1].strip
        end
    end    
    return jobid, nnodes, ntasks, query, dbsize, run, nsplits, nrecords
end

#puts "JOB_ID;HADOOP_JOB;NNODES;NPROCESSORS;QUERY;RUN;DB SIZE;NTASKS;NFRAGMENTS;NSPLITS;NRECORDS;PARTITIONING_TIME;LOAD_PARTIALS_TIME;COMBINE_PARTIALS_TIME;TRANSFER_RESULT;TOTAL_TIME;OUTPUT SIZE;ERRORS"


def process_one_dir(dir)
    Dir.glob(dir + '/*.txt') do |log_file|

        File.open(log_file, "r") do |infile|
            size = "UNKNOWN"
            jobid = "UNKNOWN"
            error = "NO"
            while (line = infile.gets)
                if (line.strip == "------------------------------------------")
                    jobid, nnodes, ntasks, query, dbsize, run, nsplits, nrecords = parse_log_header(infile)
                    next
                end
                if (line.include? "VP:partitioningTime")
                    partitioningTime = line.split("VP:partitioningTime:")[1].strip
                    partitioningTime.sub!("ms","").strip!
                end
                if (line.include? "Running job:")
                    hadoopjob = line.split("Running job:")[1].strip
                end
                if (line.include? "COMPOSING_TIME_LOAD_PARTIALS=")
                    loadpartials = line.split("COMPOSING_TIME_LOAD_PARTIALS=")[1].strip
                end   
                if (line.include? "COMPOSING_TIME_COMBINE_PARTIALS=")
                    combinepartials = line.split("COMPOSING_TIME_COMBINE_PARTIALS=")[1].strip
                end 
                if (line.include? "Copy result time:")
                    transferResult = line.split("Copy result time:")[1].strip
                    transferResult.sub!("ms.","").strip!
                end 
                if (line.include? "Total execution time:")
                    totaltime = line.split("Total execution time:")[1].strip
                    totaltime.sub!("ms.","").strip!
                end 
                if (line.include? "Size of output")
                    size = line.split(":")[1]
                    size = size.split(" ")[0].strip
                end
                if (line.include? "Error")
                    error = "YES"
                end
                if (line.include? "Failed reduce tasks")
                    if line.split("=")[1].strip.to_i > 0
                        error = "YES"
                    end
                end
                if (line.include? "Failed map tasks")
                    if line.split("=")[1].strip.to_i > 0
                        error = "YES"
                    end
                end
            end
            nfragments = nsplits.to_i * nrecords.to_i
            puts "#{jobid};#{hadoopjob};#{nnodes};#{nnodes.to_i * ntasks.to_i};#{query};#{run};#{dbsize};#{ntasks};#{nfragments};#{nsplits};#{nrecords};#{partitioningTime};#{loadpartials};#{combinepartials};#{transferResult};#{totaltime};#{size};#{error}"
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