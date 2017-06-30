#!/usr/bin/env python3

"""
Alyssa Harris

Proof of concept code to manage many fortran runs from python
Currently runs 40 executables

"""

import os
import sys
import subprocess
import csv
import threading
import time

# Executable prep done in a separate file
from executable_prep import prep_for_run

tot_plan = 10
num_slices = 4
my_list = [-1] * tot_plan * num_slices

first_lock = threading.Event()

def call_exe(plan_num, db_ptr):
	global my_list, first_lock
	exe_num = (plan_num + 1) * num_slices - (db_ptr + 1)
	src_path, exe, cmdList = prep_for_run(exe_num)
	exe_path = cmdList[0]
	# call the executable
	try:
		if plan_num == 0:
			# print("I'm about to sleep! \n")
			# time.sleep(12)
			# print ("Now call subprocess \n")
			my_list[exe_num] = subprocess.call(cmdList, stdout=subprocess.PIPE) # subprocess.check_output(cmdList)
			# print("Wake up main threads! \n")
			# first_lock.set()
		else:
			# time.sleep(12)
			# print ("Plan number {} about to execute".format(exe_num))
			my_list[exe_num] = subprocess.call(cmdList, stdout=subprocess.PIPE) # subprocess.check_output(cmdList)
	except OSError as e:
		errno, strerror = e.args
		print("OS error({0}): {1}".format(errno,strerror))
		print("When trying to access: {}".format(cmdList))
	except TypeError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except IndexError as e:
		print(str(sys.exc_info()[0]) + ': ' + str(e))
	except:
		print(str(sys.exc_info()[0]))

def sum_groups(plan):
	json_list = []
	for csvR in range(0, num_slices):
		json_list.append(get_results(csvR))


def manager():
	global help_list
	start = time.time()
	thread_list = []

	for plan in range(0,tot_plan):
		for db_index in range(0, num_slices):
			t = threading.Thread(target = call_exe, name = str(plan), args = (plan,db_index,))
			thread_list.append(t)
			t.start()

	# print("Main thread about to wait")
	# first_lock.wait()
	# print("*********")

	# print("MAIN THREAD IS BACK")
	# Here is where the second portion of the first executable will be called.

	# """
	for t_ptr in thread_list:
		t_ptr.join()
	"""

	# This is functionally the same as above

	while sum(my_list) != 0:
		# print("******** ")
		# print("My list is: \n {}".format(my_list))
		time.sleep(0)
	# """
	end = time.time()
	print("Done, and elapsed time is {} and my_list is: \n {}".format(end-start, my_list))

manager()
