import unicodecsv
from pymongo import MongoClient

class generic_converter:

    def __init__(self):
        self.header_dict = {}

    def retrieve_headers(self, test_dict, name_var):
        for element in test_dict:
            if isinstance(test_dict[element], dict):
                self.retrieve_headers(test_dict[element], name_var +
                                      '||' + element)
            else:
                self.header_dict[name_var + '||' + element] = test_dict[element]

    def converter_main(self, csv_writer):
        # client = MongoClient()
        client = MongoClient('mongodb://192.168.1.31:27017')
        db = client['tessel']
        collection_obj = db['sensors']
        cursor_records = collection_obj.find()
        header_list = []

        for cursor in cursor_records:
            self.retrieve_headers(cursor, '')
            for item_label in self.header_dict:
                if item_label not in header_list:
                    header_list.append(item_label)
            self.header_dict = {}
        csv_writer.writerow(header_list)

        cursor_records = collection_obj.find()
        for cursor in cursor_records:
            row_to_push = []
            self.header_dict = {}
            self.retrieve_headers(cursor, '')
            for item_label in header_list:
                if item_label in self.header_dict:
                    row_to_push.append(self.header_dict[item_label])
                else:
                    row_to_push.append('')
            csv_writer.writerow(row_to_push)


def main():
    file_name = '/Users/medinazari/Desktop/dump.csv'
    f_write = open(file_name, 'wb')
    csv_writer = unicodecsv.writer(f_write, delimiter=',', quotechar='"')
    converter_object = generic_converter()
    converter_object.converter_main(csv_writer)

if __name__ == '__main__':
    main()
