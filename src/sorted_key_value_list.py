# The bucket holds the (key, value) pairs in a sorted list
# class node represents a node of that sorted list
class node:
	# Initialize this node to store the (k,v) key-value pair and point to the next node n
	def __init__(self, k, sk, v, n):
		self.key = k
		self.split_order_key = sk
		self.value = v
		self.next = n

	# Getters
	def get_key(self):
		return self.key

	def get_split_order_key(self):
		return self.split_order_key

	def get_value(self):
		return self.value

	def get_next(self):
		return self.next

	# Setters
	def set_key(self, k):
		self.key = k

	def set_split_order_key(self, sk):
		self.split_order_key = sk

	def set_value(self, v):
		self.value = v

	def set_next(self, n):
		self.next = n





# This is the list that keeps the (key,value) pairs in sorted order.
# The order is on the split-order of the key
class sorted_key_value_list:
	def __init__(self):
		self.head = None

	# Add the (k,v) key-value pair in sorted order according to the split-order of the key k
	# If the key already exists then update the value
	# Return true if the key was inserted. Else return false.
	def insert(self, k, v):
		str_k = bin(k)
		str_k = str_k[2:]
		str_k = '0'*(64 - len(str_k)) + str_k
		reverse_str_k = str_k[::-1]
		split_order_key = long(reverse_str_k, 2)

		if self.head is None:
			self.head = node(k, split_order_key, v, None)
			return True
		else:
			previous = None
			current = self.head
			while (current != None) and current.get_split_order_key() < split_order_key:
				previous = current
				current = current.get_next()
			# Test if the key is already there
			if (current != None) and (current.get_key() == k):
				current.set_value(v)
				return False 
			new_node = node(k, split_order_key, v, current)
			if previous == None:
				self.head = new_node
			else:
				previous.set_next(new_node)
			return True

	# Return the value associated with the key. If the key is not present then None is returned
	def get(self, k):
		current = self.head
		while current != None:
			if current.get_key() == k:
				return current.get_value()
			current = current.get_next()
		return None

	# Remove the key-value pair with key k 
	# Return true if the key was present. Otherwise return false.
	def remove(self, k):
		previous = None
		current = self.head

		while (current != None) and (current.get_key() != k):
			previous = current
			current = current.get_next()
		
		if current != None:
			# Key was found
			if previous == None:
				# Removing head of the list
				self.head = current.get_next()
			else:
				previous.set_next(current.get_next())
			del current
			return True
		else:
			# Key not found
			return False

	# Split the list according to the i-th LSB bit (i >= 1)
	# This operation removes the sublist that in the i-th LSB bit has a 1
	def split(self, i):
		previous = None
		current = self.head

		while (current != None):
			str_key = bin(current.get_key())
			str_key = str_key[2:]
			str_key = '0'*(64 - len(str_key)) + str_key
			str_key = str_key[::-1]
			if str_key[i-1] == '1':
				break
			previous = current
			current = current.get_next()
		
		if current != None:
			if previous == None:
				self.head = None
			else:
				previous.set_next(None)
			return current
		else:
			return None	
				
	def not_split(self, i):
		current = self.head
		while (current != None):
			str_key = bin(current.get_key())
			str_key = str_key[2:]
			str_key = '0'*(64 - len(str_key)) + str_key
			str_key = str_key[::-1]
			if str_key[i-1] == '1':
				return current
			current = current.get_next()
		return None
		
	def get_all(self):
		current = self.head
		l = []
		while current != None:
			l.append(current.get_key())
			l.append(current.get_value())			
			current = current.get_next()	
		return l
