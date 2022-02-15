class buffer:

    data = []
    stored_page_number = -1  

    def get_data(self, position):
        return self.data[position]

    def get_page_number(self):
        return self.stored_page_number

    def set_page_number(self, number):
        self.stored_page_number=number
    
    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __len__(self):
        return len(self.data)

    def clear(self):
        self.data.clear()

    def append(self, value):
        self.data.append(value)
    