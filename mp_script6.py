"""
Script to generate a new set of hashes for articles in the PMC OA subset.
"""

from datetime import datetime
import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple
import multiprocessing as mp
from queue import Empty
from multiprocessing import Queue, Process, Value
from multiprocessing.synchronize import Lock, Semaphore
from multiprocessing.connection import Connection
import numpy as np
import pandas as pd
import requests
import sys
import time
from tqdm import tqdm

## Constants

with open("ncbi_api_key.txt", 'r') as f:
	API_KEY = f.read().strip('\n')

OA_FILES_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt"
OA_FILES_PATH = "oa_file_list.txt"
OA_EXPECTED_SIZE = 723799944
N_PAPERS_BLOCK = int(300)
OUTPATH = "hashes.tsv"
BASE_QUERY = (
	"https://www.ncbi.nlm.nih.gov/research/bionlp/"
	"RESTful/pmcoa.cgi/BioC_json/{pmcids}/unicode"
	f"?api_key={API_KEY}"
)
N_PROC = 10
PASSAGE_HASH_LEN = 2
HASHES_PER_FILE = int(2.5e5)

## Helpers

def download_with_progress(
	url: str,
	block_size: int = int(1e6),
	expected_file_size: int = 0
) -> Any:
	"""
	Streams large files so user can be updated of progress
		(	
			https://stackoverflow.com/questions/37573483/
			progress-bar-while-download-file-over-http-with-requests
		)
	"""
	resp = requests.get(url, stream = True)

	total_size = int(resp.headers.get("content-length", expected_file_size))

	with tqdm(
		total = expected_file_size,
		unit = "B",
		unit_scale = True
	) as pbar:
		with open(OA_FILES_PATH, 'wb') as f:
			for data in resp.iter_content(block_size):
				pbar.update(len(data))
				f.write(data)

def n_running_procs(
	locks: Dict[int, Lock]
) -> int:
	"""
	"""
	n_sleeping = 0

	for lock in locks.values():
		if lock.acquire(block=False):
			lock.release()
			n_sleeping += 1

	return n_sleeping

def get_sleep_time(
	locks: Dict[int, Lock]
) -> int:
	"""
	"""
	for i in range(1, N_PROC+1):
		check_time = i*60

		if locks[check_time].acquire(block=False):
			return check_time

	raise ValueError("Shouldn't happen, not enough locks for processes")

def worker(
	in_q: "Queue[Tuple[int, str, List[str]]]",
	out_q: "Queue[Tuple[int, List[str]]]",
	sleep_locks: Dict[int, Lock]
) -> None:
	"""
	"""
	while not in_q.empty():

		try:
			block_num, base_query, pmc_ids = in_q.get(timeout = 45)
		except Empty:
			time.sleep(1)
			continue

		query_res = None
		
		try:
			query = ','.join(pmc_ids)

			query_url = base_query.format(pmcids = query)

			query_res = requests.get(
				query_url,
				timeout = 30
			).json()

		except Exception as e:
			in_q.put((block_num, base_query, pmc_ids))

			sleep_time = get_sleep_time(sleep_locks)

			n_running = n_running_procs(sleep_locks)

			print((
				f"Lost connection, sleeping for {sleep_time} seconds. "
				f"There are {n_running} running processes."
			))	

			time.sleep(sleep_time)

			sleep_locks[sleep_time].release()

			n_running = n_running_procs(sleep_locks)

			print((
				f"Restarting process. There are "
				f"{n_running} running processes."
			))

			continue

		new_hashes: List[str] = []

		seen = {}

		for paper in query_res:
			passages = paper["documents"][0]["passages"]
			pmc_id = paper["documents"][0]["id"]

			if pmc_id == "unknown":
				continue

			if pmc_id[:3] != "PMC":
				try:
					int(pmc_id)
				except ValueError:
					raise ValueError(f"Got unrecognizable PMCID '{pmc_id}'")

				pmc_id = f"PMC{pmc_id}"

			## Skip duplicate results since they seem to have the same hash
			## value when they do appear
			try:
				seen[pmc_id]
				continue
			except KeyError:
				pass

			passage_hashes: List[str] = []

			for passage in passages:
				md5 = hashlib.md5()
				md5.update(passage["text"].encode())
				passage_hashes.append(md5.hexdigest()[:PASSAGE_HASH_LEN])

			new_hashes.append(
				f"{pmc_id}\t{''.join(passage_hashes)}\n"
			)

			seen[pmc_id] = 1

		res	= (block_num, new_hashes)

		out_q.put(res)

		time.sleep(1)

if __name__ == "__main__":
	## Retrieve a new copy of the OA files list
	print("Downloading open access file list (total size is approximate)...")

	if True:
		download_with_progress(
			OA_FILES_URL,
			expected_file_size = OA_EXPECTED_SIZE
		)

	## Load file list
	print("Loading PMC IDs...")

	oa_files = pd.read_csv(
		OA_FILES_PATH,
		sep = '\t',
		skiprows = 1,
		#nrows = 10045 ## DEBUG ONLY
	)

	pmc_ids = oa_files.iloc[:,2].tolist()

	pmc_id_blocks: List[Tuple[int, str, List[str]]] = []

	for cursor in range(0, len(pmc_ids), N_PAPERS_BLOCK):
		pmc_id_blocks.append(
			(
				cursor//N_PAPERS_BLOCK, 
				BASE_QUERY,
				pmc_ids[cursor:cursor+N_PAPERS_BLOCK]
			)
		)

	## Query results

	print(f"Querying hashes for {len(pmc_ids)} manuscripts...")

	proc_results: List[Tuple[int, List[str]]] = []

	in_q: "Queue[Tuple[int, str, List[str]]]" = mp.Queue()
	out_q: "Queue[Tuple[int, List[str]]]" = mp.Queue()

	for i, block in enumerate(pmc_id_blocks):
		in_q.put(block)

	sleep_locks: Dict[int, Lock] = {}

	for i in range(1, N_PROC+1):
		sleep_locks[i*60] = mp.Lock()

	procs: List[Process] = []

	for i in range(N_PROC):
		proc = mp.Process(
			target = worker,
			args = (in_q, out_q, sleep_locks)
		)
		proc.start()
		procs.append(proc)
		time.sleep(0.5)

	with tqdm(
		total = len(pmc_ids)
	) as pbar:
		while len(proc_results) < len(pmc_id_blocks):
			new_result = out_q.get()

			proc_results.append(new_result)
			pbar.update(len(new_result[1]))

	for i, proc in enumerate(procs):
		print(f"Joining worker {i}")
		if proc.is_alive():
			print(f"\tWorker {i} was sleeping, termination sent.")
			proc.terminate()
		proc.join(timeout = 0.5)

	comb_results: List[str] = []

	for result in proc_results:
		comb_results += result[1]

	#comb_results = sorted(comb_results, key = lambda x: x.split('\t')[0])

	pmcid_order = {
		pmcid: i
		for i, pmcid in enumerate(oa_files.iloc[:,2].tolist())
	}

	comb_results = sorted(
		comb_results,
		key = lambda row: pmcid_order[row.split('\t')[0]]
	)

	## (
	##	https://stackoverflow.com/questions/31299580/
	##	python-print-the-time-zone-from-strftime
	## )

	is_dst = time.daylight and time.localtime().tm_isdst > 0
	tz = time.tzname[is_dst]

	dt = datetime.now()


	for i in range(0, max(int(len(comb_results)/HASHES_PER_FILE) + 1, 1)):
		ind1 = i * HASHES_PER_FILE
		ind2 = (i+1) * HASHES_PER_FILE

		with open(f"hashes/hashes_{i}.tsv", 'w') as f:
		#with open(f"hashes/test_hashes_{i}.tsv", 'w') as f:
			f.write(dt.strftime(f"%Y-%m-%d %T {time.tzname[is_dst]}\n"))
			f.writelines(comb_results[ind1:ind2])



















