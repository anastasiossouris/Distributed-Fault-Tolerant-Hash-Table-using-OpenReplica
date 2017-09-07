# i must concoordify the bucket_manager
from bucket_managerproxy import *
from bucketproxy import *



class hash_table:
	# i.e manager_replicas = ['127.0.0.1:14000', '127.0.0.1:14001', '127.0.0.1:14002']
	def __init__(self, manager_replicas):
		# this returns -> '127.0.0.1:14000,127.0.0.1:14001,127.0.0.1:14002'
		self.manager_replicastring =  ','.join(manager_replicas[:])	

		# Connect to the bucket_manager
		self.bucket_manager_client = bucket_manager(self.manager_replicastring)

		self.load_membership()	

	def load_membership(self):
		members = self.bucket_manager_client.membership()
		self.size = members[0]
		self.bucket_members = members[1][:]

		# Connect to the bucket clients
		self.bucket_client = []
		
		for i in range(self.size):
			bucket_client_replicastring = ','.join(self.bucket_members[i][:])
			self.bucket_client.append(bucket(bucket_client_replicastring))		
	
	def put_non_blocking(self, k, v):
		retry = 0

		index = k%self.size
		contact_bucket_client = self.bucket_client[index]
		res = None

		while True:
			try:
				res = contact_bucket_client.put(k,v)
				if res != None:
					break
			except Exception as e: 
				retry = retry + 1
				# contact_bucket_client is not responsible for this key i must follow the next key
				contact_bucket_client = bucket(str(e))			

		if retry:
			# Maybe it is a good idea to reload the membership
			self.load_membership()	

		return (res,retry)
	
	def put_non_blocking_2(self, k, v):
		retry = 0

		index = k%self.size
		contact_bucket_client = self.bucket_client[index]
		res = None

		while True:
			try:
				res = contact_bucket_client.put(k,v)
				if res != None:
					break
			except Exception as e: 
				retry = retry + 1
				# contact_bucket_client is not responsible for this key i must follow the next key
				contact_bucket_client = bucket(str(e))			

		return (res,retry)

	def remove_non_blocking(self, k):
		retry = False

		index = k%self.size
		contact_bucket_client = self.bucket_client[index]
		res = None

		while True:
			try:
				res = contact_bucket_client.remove_non_blocking(k)
				if re != None:
					break
			except Exception as e:
				retry = True
				contact_bucket_client = bucket(str(e))

		if retry:
			self.load_membership()

		return (res,retry)


	def get(self, k):
		retry = False

		index = k%self.size
		contact_bucket_client = self.bucket_client[index]
		res = None

		while True:
			try:
				res = contact_bucket_client.get(k)
				if res != None:
					break				
			except Exception as e: 
				retry = True
				contact_bucket_client = bucket(str(e))			

		if retry:
			# Maybe it is a good idea to reload the membership
			self.load_membership()	

		return (res,retry)
