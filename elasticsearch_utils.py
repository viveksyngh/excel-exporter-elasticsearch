from elasticsearch import Elasticsearch
from config import ELASTIC_SEARCH_HOST, ELASTIC_SEARCH_PORT
from elasticsearch import helpers


def create_index(index_name, document_type, id, body):
	"""Creates an index and inserts a document in elasticsearch"""
	es = Elasticsearch(host=ELASTIC_SEARCH_HOST, port=ELASTIC_SEARCH_PORT)
	try:
		response = es.index(index=index_name, doc_type=document_type, id=id, 
							body=body)
		return True
	except Exception, e:
		print str(e)
		return False


def create_index_bulk(actions):
	"""Creates indexes in bulk"""
	es = Elasticsearch(host=ELASTIC_SEARCH_HOST, port=ELASTIC_SEARCH_PORT)
	if actions:
		helpers.bulk(es, actions)


def create_index_only(index_name, document_type):
	"""Creates an index without any document entry."""
	es = Elasticsearch(host=ELASTIC_SEARCH_PORT, port=ELASTIC_SEARCH_HOST)
	try:
		response = es.index(index=index_name, doc_type=document_type)
		print response
		return True
	except Exception, e:
		print str(e)
		return False