from concoord.exception import *
from sorted_key_value_list import *


class bucket:
	def __init__(self):
		self.data = sorted_key_value_list()
		self.blocked_commands = []
		self.split_in_progress = False		
				
	# Note: ai starts with 1 (coresponding to position 0 from the right)
	# and am is in split-order -- i.e the LSB is in the left
	def set_params(self, names, ai, am, l, _concoord_command):
		self.next_bucket = list(names)
		self.i = int(ai)
		self.lsb_match = str(am)

		if l != None:
			l = list(l)
			# the service provider has provided me with a snapshot
			while len(l) > 0:
				key = l.pop(0)
				value = l.pop(0)
				self.internal_put(key, value, None)

	def internal_put(self, k, v, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception('service provider provided invalid snapshot')
		self.data.insert(k,v)

	def put(self, k, v, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception(','.join(self.next_bucket[:]))
		elif self.split_conflict(k, None):
			self.blocked_commands.append(_concoord_command)
			raise BlockingReturn()
		return self.data.insert(k, v)

	def put_non_blocking(self, k, v, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception(','.join(self.next_bucket[:]))
		elif self.split_conflict(k, None):
			return None
		return self.data.insert(k,v)

	def get(self, k, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception(','.join(self.next_bucket[:]))
		return self.data.get(k)

	def remove_non_blocking(self, k, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception(','.join(self.next_bucket[:]))
		elif self.split_conflict(k, None):
			return None
		return self.data.remove(k)

	def remove(self, k, _concoord_command):
		if not self.key_in_range(k, None):
			raise Exception(','.join(self.next_bucket[:]))
		elif self.split_conflict(k, None):
			self.blocked_commands.append(_concoord_command)
			raise BlockingReturn()
		return self.data.remove(k)

	def key_in_range(self, k, _concoord_command):
		# Tests if the key is in the bucket's allowable range
		# To do this the i-lsbs are tested against the i-lsbs of the 
		# lsb_match
		str_key = bin(k)
		str_key = str_key[2:]
		str_key = '0'*(64 - len(str_key)) + str_key
		str_key = str_key[::-1]
		str_key = str_key[:self.i]
		if str_key == self.lsb_match[:self.i]:
			return True
		else:
			return False

	def split_conflict(self, k, _concoord_command):
		# If there is a split in progress tests whether there is 
		# a conflict with this key
		if self.split_in_progress == True:
			# This is not needed because i already know that the key is in the allowable range
			# so i could just test the self.i-th bit if it is '1'.
			# But this version is tested so i leave it as is!
			lsb_mismatch = self.lsb_match[:self.i] + '1'
			str_key = bin(k)
			str_key = str_key[2:]
			str_key = '0'*(64 - len(str_key)) + str_key
			str_key = str_key[::-1]
			str_key = str_key[:self.i + 1]
			if str_key == lsb_mismatch:
				return True
			else:
				return False
		else:
			return False	

	def prepare_update_internal_state(self, _concoord_command):
		self.split_in_progress = True
		split_node = self.data.not_split(self.i + 1)
	
		if split_node == None:
			return (self.i, self.lsb_match, None, list(self.next_bucket))
		else:
			l = []
			while split_node != None:
				l.append(split_node.get_key())
				l.append(split_node.get_value())
				split_node = split_node.get_next()
			return (self.i, self.lsb_match, l, list(self.next_bucket))

	def update_internal_state(self, names, _concoord_command):
		split_node = self.data.split(self.i+1)
		while split_node != None:
			next_node = split_node.get_next()
			del split_node
			split_node = next_node
		self.split_in_progress = False
		self.lsb_match = self.lsb_match[:self.i] + '0' + self.lsb_match[self.i + 1: ]
		self.i = self.i + 1		
		self.next_bucket = list(names)
		
		if len(self.blocked_commands) > 0:
			unblocked = {}
			while len(self.blocked_commands) > 0:	
				unblockcommand = self.blocked_commands.pop(0)
				unblocked[unblockcommand] = str(','.join(self.next_bucket[:]))
			raise UnblockingReturn(unblockeddict=unblocked)	

	def get_next_bucket(self, _concoord_command):
		return ','.join(self.next_bucket[:])

	def get_all(self, _concoord_command):
		return self.data.get_all()
