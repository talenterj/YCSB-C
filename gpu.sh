
# a='--query-gpu=pstate,memory.used,utilization.memory,utilization.gpu,encoder.stats.sessionCount'
# b='--query-gpu=encoder.stats.averageFps,encoder.stats.averageLatency,temperature.gpu,power.draw'


echo "util  tmp/C  watt  mem%  mem  lat"

while true; 
do nvidia-smi --query-gpu=utilization.gpu,temperature.gpu,power.draw,memory.used,encoder.stats.averageLatency --format=csv,noheader | tee gpu.log;

sleep 1;
done

