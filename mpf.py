from datetime import datetime
from collections import OrderedDict
mpf_task_run = OrderedDict()
mpf_task_count = OrderedDict()
mpf_task_time = OrderedDict()

def task_start(task_name):
	if(task_name in mpf_task_run):
		print("Warning: {} already started. call ignored".format(task_name))
	else:		
		mpf_task_run[task_name] = datetime.now()

def task_end(task_name):
	if (task_name in mpf_task_run):
		time_run = datetime.now() - mpf_task_run.pop(task_name)
		
		total_time = mpf_task_time.pop(task_name, None)
		if(total_time is None):
			total_time = time_run
		else:
			total_time = total_time + time_run 

		mpf_task_time[task_name] = total_time

		total_count = mpf_task_count.pop(task_name, None)
		if(total_count is None):
			total_count = 1
		else:
			total_count = total_count + 1

		mpf_task_count[task_name] = total_count		
			
	else:
		print("Warning: {} not started yet, use mpf_task_start before calling mpf_task_end. call ignored".format(task_name))

def summary():
	for task, count in mpf_task_count.items():
		total_time = mpf_task_time[task]
		avg_time = total_time / count
		print("task:{:25s} count:{:10} total time:{} avg time:{}".format(task, count, total_time, avg_time))

def clear():
	mpf_task_run.clear()
	mpf_task_count.clear()	
	mpf_task_time.clear()