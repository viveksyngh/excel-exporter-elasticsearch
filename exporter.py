import os
import xlrd
from datetime import datetime
from elasticsearch_utils import create_index_only, create_index_bulk
import argparse


def read_file_and_get_data(file_path, index_name, document_type):
	"""Returns list of row from an excel in format of elatic bulk query."""
	try:
		document_list = []
		book = xlrd.open_workbook(file_path)
	except IOError, e:
		print "File is not present at given location."
	else:
		sh = book.sheet_by_index(0)
		header = []
		nrows = sh.nrows
		ncols = sh.ncols
		header_map = {}
		for row in range(nrows):
			
			if row == 0:
				for col in range(ncols):
					header.append(sh.cell_value(rowx=row, colx=col))

				header_map = dict(enumerate(header))
				continue

			document = {
					"_index": index_name,
					"_type": document_type,
					"_id": row,
					"_source" : {
						"timestamp": datetime.now()
					}
				}

			for col in range(ncols):
					document["_source"][header_map[col]] = sh.cell_value(rowx=row, colx=col)
			document_list.append(document)
	return document_list


def export_excel(file_path, index_name, document_type):
	"""Dumping data in bulk to elasticsearch in chunk of 5000."""
	data = read_file_and_get_data(file_path, index_name, document_type)
	print len(data)
	for i in range(0, len(data), 5000):
		try:
			create_index_bulk(data[i : i + 5000])
		except Exception, e:
			print str(e)


def get_args():
    '''This function parses and return arguments passed in'''
    # Assign description to the help doc
    parser = argparse.ArgumentParser(
        description='Script retrieves details to export an excel to elastisearch')
    # Add arguments
    parser.add_argument(
        '-f', '--filepath', type=str, help='File path', required=True)
    parser.add_argument(
        '-i', '--index', type=str, help='Index name', required=True)
    parser.add_argument(
        '-d', '--doctype', type=str, help='Document type', required=True)
    # Array for all arguments passed to script
    args = parser.parse_args()
    # Assign args to variables
    filepath = args.filepath
    index = args.index
    doctype = args.doctype
    # Return all variable values
    return filepath, index, doctype


if __name__ == '__main__':
	file_path, index_name, document_type = get_args()
	export_excel(file_path, index_name, document_type)

