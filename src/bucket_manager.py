class bucket_manager:
	def __init__(self):
		pass

	def set_params(self, vs, rs, b):
		self.virtual_size = int(vs)
		self.real_size = int(rs)
		self.bucket = list(b) # a list of lists of strings

	def membership(self):
		return (self.virtual_size, self.bucket[:])
	
	def prepare_add_bucket(self):
		victim_bucket = self.real_size%(self.virtual_size/2)
		return (self.bucket[victim_bucket][:], self.bucket[victim_bucket+1][:], victim_bucket)

	def add_bucket(self, names):
		victim_bucket = self.real_size%(self.virtual_size/2)

		if victim_bucket != 0:
			self.real_size = self.real_size + 1
			self.bucket[self.real_size - 1] = list(names)
		else:
			self.virtual_size = self.virtual_size*2
			self.bucket = self.bucket + self.bucket
			self.real_size = self.real_size + 1
			self.bucket[self.real_size - 1] = list(names)
			
			for i in range(self.real_size,len(self.bucket)):
				self.bucket[i] = self.bucket[victim_bucket + 1][:]
				victim_bucket = victim_bucket + 1
