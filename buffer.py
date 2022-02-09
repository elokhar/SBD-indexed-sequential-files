from record import record

class buffer:

    data = []
    stored_page_number = -1
    buffered_file = None

    def get_data(self, position):
        return self.data[position]

    def get_page_number(self):
        return self.buffer_number

    def set_page_number(self, number):
        self.stored_page_number=number

    