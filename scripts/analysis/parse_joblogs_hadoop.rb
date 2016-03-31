#!/usr/bin/env ruby
# Gabriel Tessarolli

class Job
    attr_accessor :id, :submit_time, :launch_time, :finish_time, :total_maps, :total_reduces, 
        :setup_start_time, :setup_finish_time, :errors
    attr_reader :tasks

    def initialize()
        @jobid="UNKNOWN"
        @submit_time="UNKNOWN"
        @launch_time="UNKNOWN"
        @finish_time="UNKNOWN"
        @total_maps="UNKNOWN"
        @total_reduces="UNKNOWN"
        @tasks=Hash.new
        @errors=false
    end
    
    def add_task(task)
        if !(@tasks.has_key? task.id)
            @tasks[task.id] = task     
        end
    end
    
    def get_task(taskid)
        @tasks[taskid]
    end
    
    def get_setup_time()
        setup_finish_time.to_i - setup_start_time.to_i
    end
    
    def get_total_time()
        finish_time.to_i - launch_time.to_i 
    end
    
    def process_tasks()
        alltimestamps = Array.new
        atasks = @tasks.values
        hosts = Array.new
        atasks.each do |t|
            next if t.type == "REDUCE"
            r = alltimestamps.assoc(t.start_time)
            if (r != nil)
                r[1] = r[1] + 1
            else
                alltimestamps.push([ t.start_time, +1 ])
            end
            
            r = alltimestamps.assoc(t.finish_time)
            if (r != nil) 
                r[1] = r[1] - 1
            else
                alltimestamps.push([ t.finish_time, -1 ])
            end
            hostname = t.host.sub("/default-rack/","")
            h = hosts.assoc(hostname)
            if (h != nil)
                h[1] = h[1] + t.get_total_time
            else
                hosts.push([ hostname, t.get_total_time ])
            end
        end
        count = 0
        #puts "#{alltimestamps}"
        alltimestamps.sort_by! { |e| e[0] }
        #puts "sorted: #{alltimestamps}"
        usage = Array.new
        alltimestamps.each do |r|
            count = count + r[1]
            time = r[0].to_i - @launch_time.to_i
            puts "#{time};#{count}"
        end

        puts "host;loadtime"
        hosts.each do |h|
            puts "#{h[0]};#{h[1]}"
        end
   end
end

class Task
    attr_accessor :id, :type, :start_time, :finish_time, :host
    
    def initialize()
        @id = "UNKNOWN"
        @type = "UNKNOWN"
        @start_time = "UNKNOWN"
        @finish_time = "UNKNOWN"
        @host = "UNKNOWN"
    end

    def get_total_time()
        finish_time.to_i - start_time.to_i
    end
end

def process_file(file)
    job = Job.new
    File.open(file,"r") do |infile|
        # use ".\n" as delimiter for registers in log file
        while (line = infile.gets(".\n"))
            case line
                when /Job JOBID=\"(\w+?)\" .* SUBMIT_TIME=\"(\d+?)\" .*/
                    job.id=$1
                    job.submit_time=$2
                when /Job JOBID=\"\w+?\" LAUNCH_TIME=\"(\d+?)\" TOTAL_MAPS=\"(\d+?)\" TOTAL_REDUCES=\"(\d+?)\" .*/
                    job.launch_time=$1
                    job.total_maps=$2
                    job.total_reduces=$3
                when /Job JOBID=\"\w+?\" FINISH_TIME=\"(\d+?)\" JOB_STATUS=\"(\w+?)\" .*/
                    job.finish_time=$1
                    job.errors = true if $2 != "SUCCESS"
                when /Task TASKID=\"(\w+?)\" TASK_TYPE=\"(\w+?)\" START_TIME=\"(\d+?)\" .*/
                    if ($2 == "MAP" or $2 == "REDUCE")
                        task = Task.new
                        task.id=$1
                        task.type=$2
                        task.start_time=$3
                        job.add_task(task)
                    elsif ($2 == "SETUP")
                        job.setup_start_time=$3
                    end
                when /Task TASKID=\"(\w+?)\" TASK_TYPE=\"(\w+?)\" TASK_STATUS=\"(\w+?)\" FINISH_TIME=\"(\d+?)\" .*/
                    next if $3 != "SUCCESS"
                    if ($2 == "MAP" or $2 == "REDUCE")
                        task = job.get_task($1)
                        task.finish_time=$4
                    elsif ($2 == "SETUP")
                        job.setup_finish_time=$4
                    end
                when /MapAttempt .* TASKID=\"(\w+?)\" .* TASK_STATUS=\"(\w+?)\" .* HOSTNAME=\"(\S+?)\" .m*/
                    if $2 != "SUCCESS"
                        job.errors = true
                    end
                    task = job.get_task($1)
                    next if !task
                    task.host = $3
                when /ReduceAttempt .* TASKID=\"(\w+?)\" .* TASK_STATUS=\"(\w+?)\" .* HOSTNAME=\"(\S+?)\" .m*/
                    if $2 != "SUCCESS"
                        job.errors = true
                    end
                    task = job.get_task($1)
                    next if !task
                    task.host = $3                
                else
                    #puts "skipped line: #{line}"
            end
        end   
    end
    puts "#{job.id};#{job.total_maps};#{job.total_reduces};#{job.get_setup_time};#{job.get_total_time};#{job.errors}"
    puts "#{job.tasks.size}"
    job.process_tasks
end

def main(file)
    process_file(file)
end

if ARGV.size < 1
    abort "usage: #{File.basename($0)} <file>"
end

main ARGV[0]
