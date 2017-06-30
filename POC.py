#!/usr/bin/env python3

"""
Alyssa Harris

Proof of concept code to manage many fortran runs from python
Currently runs 40 executables

The four executables in a group are assumed to be related, and so they're referred to as part of one "plan"
Thus, part of this POC is to analyze each group of results while letting each executable run separately

"""

import os
import sys
import subprocess
import csv
import threading
import time
from queue import Queue

# Executable prep done in a separate file
from executable_prep import prep_for_run

tot_plan = 10
num_slices = 4
my_list = [-1] * tot_plan * num_slices
exes_complete = [[-1]*num_slices]*tot_plan
completed_event = threading.Event()
plan_queue = Queue()
plans_complete = []
all_results = {}

event_lock = threading.Lock()

def call_exe(plan_num, slice_ptr):
	global my_list, event_lock
	global exes_complete, plans_complete
	global completed_event
	exe_num = (plan_num + 1) * num_slices - (slice_ptr + 1)
	src_path, exe, cmdList = prep_for_run(exe_num)
	exe_path = cmdList[0]
	# call the executable
	try:
		my_list[exe_num] = subprocess.call(cmdList, stdout=subprocess.PIPE) # subprocess.check_output(cmdList)]
		exes_complete[plan_num][slice_ptr] = my_list[exe_num] # threads are exiting here and then re-calling the main thread
		if sum(exes_complete[plan_num])==0 and plan_num not in plans_complete:
			with event_lock:
				if sum(exes_complete[plan_num])==0 and plan_num not in plans_complete:
					plans_complete.append(plan_num)
					plan_queue.put(plan_num)
					completed_event.set()

	except OSError as e:
		errno, strerror = e.args
		print("OS error({0}): {1}".format(errno,strerror))
		print("When trying to access: {}".format(cmdList))
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
	# filler code to show we can do something to a group of related
	# 	json objects (or dictionaries)
	for slice_index in range(0, num_slices):
		json_list = [{'variable':1},{'variable':1},{'variable':1}]
	for k, v in json_list[0].items():
		all_results[k + str(plan)] = v + json_list[1][k] + json_list[2][k]
	return all_results

def manager():
	global plans_complete
	global completed_event
	global exes_complete

	try:
		start = time.time()
		thread_list = []

		for plan in range(0,tot_plan):
			for slice_index in range(0, num_slices):
				t = threading.Thread(target = call_exe, name = str(plan), args = (plan,slice_index,))
				thread_list.append(t)
				t.start()

		queue_threads = []
		# print("Main thread about to wait")
		while len(plans_complete) < 10:
			completed_event.wait()
			plan = plan_queue.get()
			t = threading.Thread(target = sum_groups, name = str(plan), args = (plan,))
			queue_threads.append(t)
			t.start()
			with event_lock:
				if plan_queue.empty():
					completed_event.clear()

		for t_ptr in queue_threads:
			t_ptr.join()
		for t_ptr in thread_list:
			t_ptr.join()
	except NameError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except:
		# just put this here so the terminal doesn't freeze
		pass
	# print("*********")

	# print("MAIN THREAD IS BACK")

	# """
	"""
	# This is functionally the same as above

	while sum(my_list) != 0:
		# print("******** ")
		# print("My list is: \n {}".format(my_list))
		time.sleep(0)
	# """
	end = time.time()
	print("All results: {}".format(all_results))
	print("Done, and elapsed time is {} and my_list is: \n {}".format(end-start, my_list))

manager()
