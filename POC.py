#!/usr/bin/env python3

"""
Alyssa Harris

Proof of concept code to manage many fortran runs from python
Currently runs 40 executables

The four executables in a group are assumed to be related, and so they're referred to as part of one "plan"
So part of this POC is to analyze each group of results while letting each executable run separately

"""

import os
import sys
import subprocess
import threading
import time
# Following requires python 3.x
from queue import Queue

# Executable prep and dealing with results done in a separate file
from executable_prep import prep_for_run
from helperfuncs import get_results, validate_results

import logging

LOG_FILENAME = 'manager_log.out'
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.DEBUG,
	filemode = 'w'
)

tot_plan = 10
num_slices = 4

exes_complete = []

# hold the output files, which I'll build in another file

output_files = [None]*tot_plan*num_slices

for i in range(0, tot_plan):
	exes_complete.append([-1]*num_slices)

plan_queue = Queue()
plans_complete = []
plans_queued = []
all_results = {}

# hold the output files, which I'll build in another file
output_files = [None]*tot_plan*num_slices
# use a lock to avoid race conditions relative to plans_complete
event_lock = threading.Lock()
# use an event flag to indicate that a plan has just completed
completed_event = threading.Event()

def call_exe(plan_num, slice_ptr):
	global event_lock
	global exes_complete, plans_complete
	global completed_event
	exe_num = (plan_num + 1) * num_slices - (slice_ptr + 1)
	cmdList, output_file = prep_for_run(exe_num)
	output_files[exe_num] = output_file
	# call the executable
	try:
		exes_complete[plan_num][slice_ptr] = subprocess.call(cmdList, stdout=subprocess.PIPE)
		logging.debug("Completed {} of {}".format(slice_ptr, plan_num))
		if sum(exes_complete[plan_num])==0 and plan_num not in plans_complete:
			with event_lock:
				if sum(exes_complete[plan_num])==0 and plan_num not in plans_complete:
					logging.debug("Putting {} in the queue".format(plan_num))
					plan_queue.put(plan_num)
					plans_complete.append(plan_num)
					completed_event.set()

	except OSError as e:
		errno, strerror = e.args
		print("OS error({0}): {1}".format(errno,strerror))
		print("When trying to execute: {}".format(cmdList))
	except TypeError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except IndexError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except NameError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except:
		print(str(sys.exc_info()[0]))

def sum_groups(plan):
	global all_results
	json_list = []
	for slice_index in range(0, num_slices):
		json_list.append(get_results(output_files[(plan + 1) * num_slices - (slice_index + 1)]))
	for k, v in json_list[0].items():
		all_results[k + str(plan)] = v + json_list[1][k] + json_list[2][k]
	logging.debug(("All results after {}: \n {}".format(plan, all_results)))

def manager():
	global completed_event

	start = time.time()
	thread_list = []

	for plan in range(0,tot_plan):
		for slice_index in range(0, num_slices):
			t = threading.Thread(target = call_exe, name = str(plan), args = (plan,slice_index,))
			thread_list.append(t)
			t.start()

	queue_threads = []

	while len(plans_complete) < tot_plan:
		completed_event.wait()
		plan = plan_queue.get()
		logging.debug(str(plans_complete))
		t = threading.Thread(target = sum_groups, name = str(plan), args = (plan,))
		queue_threads.append(t)
		t.start()
		with event_lock:
			if plan_queue.empty():
				completed_event.clear()

	# At this point, we have all the plans in the queue, so now process the results for all of them.
	while not plan_queue.empty():
		plan = plan_queue.get()
		logging.debug(str(plans_complete))
		t = threading.Thread(target = sum_groups, name = str(plan), args = (plan,))
		queue_threads.append(t)
		t.start()
	for t_ptr in queue_threads:
		t_ptr.join()

	end = time.time()

	print("Time elapsed: {}".format(end - start))

	# Make sure these are the results we need
	validate_results(tot_plan, all_results)

manager()
