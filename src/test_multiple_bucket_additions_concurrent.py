import shlex, subprocess
import time, threading
import numpy, random
from timeit import default_timer as timer
from bucket_managerproxy import *
from bucketproxy import *
from hash_table import hash_table


R = 3 # number of replicas per RSM
num_buckets = 8  # maximum size of hash-table = 8
num_buckets_started = 0 # how many has the service provider started
keys_per_client = 10
num_client_threads = 4
keys = range(1,keys_per_client*num_client_threads) # the keys that i will insert in the hash-table

# Now i determine the replicastrings
port = 14000
ip = '127.0.0.1'

replica_names = [] # a list of lists. [0] is for the manager. [1] is for bucket[0], [2] for bucket[1] etc
replicas = [] # the processes so that i can kill them at the end

replicastring = []

print 'Initiating hash-table manager'
replicas.append(subprocess.Popen(['/home/tassos_souris/concoord-workspace/module/bin/concoord', 'replica',
                                     '-o', 'bucket_manager.bucket_manager',
                                     '-a', ip, '-p', str(port)]))
replicastring.append(ip + ':' + str(port))
master_port = port
port = port + 1

time.sleep(1) # wait for the master to be established

for i in range(1,R):
	replicas.append(subprocess.Popen(['/home/tassos_souris/concoord-workspace/module/bin/concoord', 'replica',
                                  	'-o', 'bucket_manager.bucket_manager',
                                   	 '-a', ip, '-p', str(port),
                                   	 '-b', ip + ':' + str(master_port)]))
	replicastring.append(ip + ':' + str(port))
	port = port + 1

replica_names.append(replicastring)

print 'Done!'

print 'Initiating bucket replicas'
for i in range(num_buckets):
	print '\t...Starting replicas for bucket', i
	replicastring = []
	replicas.append(subprocess.Popen(['/home/tassos_souris/concoord-workspace/module/bin/concoord', 'replica',
		                             '-o', 'bucket.bucket',
		                             '-a', ip, '-p', str(port)]))
	replicastring.append(ip + ':' + str(port))
	master_port = port
	port = port + 1

	time.sleep(1) # wait for the master to be established

	for i in range(1,R):
		replicas.append(subprocess.Popen(['/home/tassos_souris/concoord-workspace/module/bin/concoord', 'replica',
		                          	'-o', 'bucket.bucket',
		                           	 '-a', ip, '-p', str(port),
		                           	 '-b', ip + ':' + str(master_port)]))
		replicastring.append(ip + ':' + str(port))
		port = port + 1

	replica_names.append(replicastring)
	print '\t...Done!'


print 'Finished initiating replicas'
print 'The replica names are:'
print str(replica_names)

print 'Waiting for replicas to be established...'
time.sleep(5)

print 'Creating hash-table manager client'
manager_replicastring =  ','.join(replica_names[0][:])
manager = bucket_manager(manager_replicastring)

print 'Creating bucket clients'
bucket_client = []

for i in range(num_buckets):
	print '\t...Creating client for bucket', i
	bucket_replicastring = ','.join(replica_names[1+i][:])
	bucket_client.append(bucket(bucket_replicastring))

print 'Setup : hash-table of size 2'
manager.set_params(2,2,replica_names[1:3])

# start bucket[0]
bucket_client[0].set_params(replica_names[2][:], 1, '0' + '1'*63, None)

# start bucket[1]
bucket_client[1].set_params([], 1, '1'*64, None)

num_buckets_started = 2

for k in keys:
	bucket_client[k%2].put(k,k)
print 'Done'


lock = threading.Lock()
lock.acquire()
count = 0
lock.release()
client_thread_do_put_event = threading.Event()

client_thread_do_put_event.clear()

random.seed(1337)
random.shuffle(keys)

class client_thread(threading.Thread):
	def __init__(self, begin, end):
		threading.Thread.__init__(self)
		self.begin = begin
		self.end = end

	def run(self):
		global lock
		global count
		global client_thread_do_put_event
		global replica_names
		global retry
		global keys

		# Create my hash-table client
		ht = hash_table(replica_names[0][:])

		# Inform the main_thread that i have started
		lock.acquire()
		count = count + 1
		lock.release()

		# Wait until we are told to start
		client_thread_do_put_event.wait()

		for k in keys[self.begin:self.end]:
			res = ht.put_non_blocking(k,0)
			assert (res[0] == False), "error in thread client"
						

try:

	dhts = []
	b = 0
	e = keys_per_client
	for i in range(num_client_threads):
		t = client_thread(b,e)
		t.start()
		dhts.append(t)
		b = b + keys_per_client
		e = e + keys_per_client

	init_done = 0
	while not init_done:
		lock.acquire()
		if count == num_client_threads:
			init_done = 1
		lock.release()

	# tell the client_thread to proceed
	print 'Telling the client thread to do the put operation'
	client_thread_do_put_event.set()

	# Make now the bucket additions
	while True:		
		#print 'Have started', num_buckets_started, 'out of', num_buckets	
		# Print the state of the buckets	
		members = manager.membership()
		size = members[0]
		bucket_members = members[1][:]
		# Connect to the bucket clients
		del bucket_client[:]
		bucket_client = []
		for i in range(size):
			bucket_client_replicastring = ','.join(bucket_members[i][:])
			bucket_client.append(bucket(bucket_client_replicastring))		
		if num_buckets_started == num_buckets:
			break	
		ps = manager.prepare_add_bucket()
		victim_bucket_name = list(ps[0])
		victim_bucket_client = bucket(','.join(victim_bucket_name[:]))
		new_bucket_name = replica_names[num_buckets_started + 1][:]
		new_bucket_client = bucket(','.join(new_bucket_name[:]))
		next_bucket_name = list(ps[1])
		num_buckets_started = num_buckets_started + 1
		# getting the snapshot from the victim bucket
		victim_bucket_snapshot = victim_bucket_client.prepare_update_internal_state()
		ai = victim_bucket_snapshot[0] + 1
		am = victim_bucket_snapshot[1]
		am = am[:ai-1] + '1' + am[ai:]
		if len(am) != 64:
			raise Exception('invalid length in am')
		# setup the new_bucket
		next_bucket_name = list(victim_bucket_snapshot[3])
		new_bucket_client.set_params(next_bucket_name[:], ai, am, victim_bucket_snapshot[2])	
		# inform the victim bucket of the change
		victim_bucket_client.update_internal_state(new_bucket_name[:])
		# tell the manager of the addition
		manager.add_bucket(new_bucket_name[:])


	# Now wait for them
	for dhts_client in dhts:
		dhts_client.join()

	ht = hash_table(replica_names[0][:])

	print 'Testing keys'
	for k in keys:
		print k, ht.get(k)
		assert (ht.get(k)[0] == 0), ""

	print 'Keys are OK!'
except Exception as e:
	print str(e)

finally:
	print 'Waiting before killing replica processes'
	time.sleep(1000)
	print 'Killing replica processes'
	for p in (replicas):
		p.kill()
