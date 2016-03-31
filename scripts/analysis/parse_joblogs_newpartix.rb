#!/usr/bin/env ruby
# Gabriel Tessarolli

def process_file(file)
    File.open(file,"r") do |infile|

        alltimestamps = Array.new
        work = Array.new
    	puts "timestamp;processorCount"
        while line = infile.gets
            matches = line.match /Worker (\d+) (END|START) (\d+)/
            next if matches == nil
            timestamp = matches[3].to_i
            worker_id = matches[1].to_i
            if matches[2] == "START"
            then
                # the r register is like this
                # r[0] - timestamp
                # r[1] - number os processors to update count
                # r[2] - processors started during this timestamp
                # r[3] - processors ended during this timestamp
	            r = alltimestamps.assoc(timestamp)
	            if (r != nil)
	                r[1] = r[1] + 1
	                r[2].push(worker_id)
	            else
	                alltimestamps.push([ timestamp, +1, [worker_id], [] ])
	            end

                #
                # r[0] - worker_id
                # r[1] - sum of work
                # r[2] - last start timestamp
                r = work.assoc(worker_id)
                if (r != nil) 
                    r[2] = timestamp
                else
                    work.push([worker_id, 0, timestamp])
                end
            else
	            r = alltimestamps.assoc(timestamp)
	            if (r != nil) 
	                r[1] = r[1] - 1
	                r[3].push(worker_id)
	            else
	                alltimestamps.push([ timestamp, -1, [], [worker_id] ])
	            end

                r = work.assoc(worker_id)
                if (r != nil) 
                    r[1] = r[1] + (timestamp - r[2])
                    r[2] = 0
                else
                    puts "something wroooooooong"
                end
	        end
        end   
        count = 0
        alltimestamps.sort_by! { |e| e[0] }
        init_time = alltimestamps.first[0].to_i
        usage = Array.new
        alltimestamps.each do |r|
            count = count + r[1]
            time = r[0].to_i
           	r[3].each do |node|
           		usage.delete(node)
            end
          	usage.concat(r[2])
            usage.sort!
            #puts "#{time};#{count};#{usage}"
            puts "#{time-init_time};#{count}"
        end
        puts "workerId;time"
        work.each do |w|
            puts "#{w[0]};#{w[1]}"
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
