class Job
  def initialize(job_name)
    @job_name = job_name
  end

  def run
    while true
      sleep((rand(100)+1)/101.0)
      host = "http://musicvideobuger.appspot.com/"
      result = `curl --silent #{host}#{@job_name}`


      if result.match(/refresh/) || result.match(/Traceback/) || result.match(/error/)
        if result.match(/Traceback/)
          p result 
        else
          p "********** Runing job *****************"
        end
      else
        p "*********** Job Complete ***************"
        p result
      end
    end
  end
end

if ARGV[0]
  p "Trying to run job: #{ARGV[0]}"
    
  job = Job.new(ARGV[0])
  process_count = ARGV[1].to_i
  
  if process_count.nil? || process_count == 0
    process_count = 10
  end
  
  process_count.times do
    fork do 
      job.run()
    end
  end

  Process.waitall
else
  p "Please specify job url"
end
