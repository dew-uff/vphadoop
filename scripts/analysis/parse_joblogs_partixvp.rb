#!/usr/bin/env ruby
# Gabriel Tessarolli

def process_file(file)
    File.open(file,"r") do |infile|
        
        # the r register is like this
        # r[0] - timestamp
        # r[1] - number os processors to update count
        # r[2] - processors started during this timestamp
        # r[3] - processors ended during this timestamp
        processors = nil

        count = 0

    	puts "timestamp;processorCount;usage"
        while line = infile.gets
        	matches = line.match /# fragments      : (\d+)/
        	if matches != nil
        	then 
        		count = matches[1].to_i
        		processors = Array.new(count)
        	else
	            matches = line.match /thread: (\d+),.* -- Execution Time: (\d+)/
	            next if matches == nil
	            count = count - 1
	            processors[matches[1].to_i] = matches[2].to_i
	            puts "#{matches[2]};#{count}"
		    end
        end

        count = 0
        puts "processor;execution_time"
        processors.each do |p|
        	puts "#{count};#{p}"
        	count = count + 1
        end
    end
end

def main(file)
    process_file(file)
end

if ARGV.size < 1
    abort "usage: #{File.basename($0)} <file>"
end

main ARGV[0]
