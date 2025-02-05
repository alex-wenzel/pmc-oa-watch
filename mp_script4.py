"""
Script to generate a new set of hashes for articles in the PMC OA subset.
"""

from datetime import datetime
import hashlib
from typing import Any, List, Optional, Tuple
import multiprocessing as mp
from multiprocessing import Queue, Process
from multiprocessing.synchronize import Lock, Semaphore
import multiprocessing

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
N_PROC = 24

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

def get_sleep_time(
	n_sleeping: "mp.Value" #type: ignore
) -> int:
	return max(n_sleeping, 1) * 60

def worker(
	in_q: "Queue[Tuple[int, str, List[str]]]",
	out_q: "Queue[Tuple[int, List[str]]]",
	n_sleeping: "mp.Value" #type: ignore
) -> None:
	"""
	"""
	while not in_q.empty():
		block_num, base_query, pmc_ids = in_q.get(timeout = 1)

		query = ','.join(pmc_ids)

		query_url = base_query.format(pmcids = query)

		query_res = None

		while query_res is None:
			try:
				query_res = requests.get(
					query_url,
					timeout = 20
				).json()
			except Exception as e:
				sleep_time = get_sleep_time(n_sleeping.value)
				n_sleeping.value += 1
				print((
					f"Lost connection, sleeping for {sleep_time} seconds. "
					f"There are {N_PROC - n_sleeping.value} running processes."
				))
				time.sleep(sleep_time)
				n_sleeping.value -= 1
				print((
					f"Restarting process. There are "
					f"{N_PROC - n_sleeping.value} running processes."
				))

		new_hashes: List[str] = []

		for paper in query_res:
			passages = paper["documents"][0]["passages"]
			pmc_id = paper["documents"][0]["id"]

			text = ''.join([
				passage["text"] for passage in passages
			])

			md5 = hashlib.md5()
			md5.update(text.encode())

			new_hashes.append(f"{pmc_id}\t{md5.hexdigest()}\n")

		res	= (block_num, new_hashes)

		out_q.put(res)

		#time.sleep(1)

if __name__ == "__main__":
	## Retrieve a new copy of the OA files list
	print("Downloading open access file list (total size is approximate)...")

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
		#nrows = 1000 ## DEBUG ONLY
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

	n_sleeping = mp.Value('i', 0)
	#n_sleeping = multiprocessing.Value('i', 0).value

	procs: List[Process] = 	[]

	for i in range(N_PROC):
		proc = mp.Process(
			target = worker,
			args = (in_q, out_q, n_sleeping)
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

	for proc in procs:
		proc.join()

	proc_results = sorted(proc_results, key = lambda x: x[0])

	comb_results: List[str] = []

	for result in proc_results:
		comb_results += result[1]

	## (
	##	https://stackoverflow.com/questions/31299580/
	##	python-print-the-time-zone-from-strftime
	## )

	is_dst = time.daylight and time.localtime().tm_isdst > 0
	tz = time.tzname[is_dst]

	dt = datetime.now()

	for i in range(0, max(int(len(comb_results)/int(5e5)), 1)):
		ind1 = int(i * 5e5)
		ind2 = int((i+1) * 5e5)

		with open(f"hashes/hashes_{i}.tsv", 'w') as f:
			f.write(dt.strftime(f"%Y-%m-%d %T {time.tzname[is_dst]}\n"))
			f.writelines(comb_results[ind1:ind2])













