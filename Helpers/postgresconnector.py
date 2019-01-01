import psycopg2
import json
import datetime
import decimal
from time import mktime
from flask import Response
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib,urllib2
import requests
from settings import hostname, username, password, database
from cdicttoxml import dicttoxml as cdtx
from dicttoxml import dicttoxml as dtx
from xml.dom.minidom import parse
import xml.dom.minidom
from lxml import etree


#Helps to decode datatypes from postgres output
class PostgresJsonEncoder(json.JSONEncoder):

    def default(self, obj):
	if isinstance(obj, datetime.datetime):
	    try:
		return (obj.strftime('%Y-%m-%d %HH:%MM:%SS'))
	    except Exception as e:
		return str(e)

	if isinstance(obj, datetime.date):
	    try:
		return (obj.strftime('%Y-%m-%d'))
	    except Exception as e:
		return str(e)

        if isinstance(obj, decimal.Decimal):
	    try:
		return float(obj)
	    except Exception as e:
		return str(e)
#            return float(obj)

        return json.JSONEncoder.default(self, obj)

class PostgresConnector:

    def __init__(self):
	try:
            self.conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
	    self.conn.autocommit=True
	    self.cur = self.conn.cursor()
	except Exception as e:
	    return str(e)

    #This function is used to check if db connection is established
    def ConnectToDatabase(self):
        try:
            self.conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
	    self.conn.autocommit = True
            return "OK"
        except Exception as e:
            return str(e)

#http://code.activestate.com/recipes/410469-xml-as-dictionary/
#APIs using

    def makeValidXML(self, xml_data):
	value = '<?xml version="1.0" encoding="utf-8"?> <exchangeRateList xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" d1p1:action="update" xmlns:d1p1="http://schemas.veson.com/2005/ImosMsg" xmlns="http://schemas.veson.com/2005/ImosData">'
	result = str(xml_data).replace('<?xml version="1.0" encoding="utf-8"?>', value)
	return result

#GET CURRENCY RATE:
    def getCurrencyRate(self, currency, date):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()
            cur = self.conn.cursor()
            where_clause = " "
            query = "SELECT Distinct ON (rcr.name::date, rc.name) (SELECT name FROM res_currency WHERE id = company.currency_id) AS baseCurrency, rc.name AS currency, rcr.rate AS rate, rcr.name::date AS effectiveDate FROM res_currency_rate rcr INNER JOIN res_currency rc ON rcr.currency_id = rc.id LEFT JOIN res_company company ON rcr.company_id = company.id " 

	    if currency and date:
	    	where_clause = "WHERE rc.active = True AND rc.name = '{}' AND rcr.name::date = '{}'".format(currency, date)  
	    elif currency:
	    	where_clause = "WHERE rc.active = True AND rc.name = '{}'".format(currency)  
	    elif date:
	    	where_clause = "WHERE rc.active = True AND rcr.name::date = '{}'".format(date)  
	    else:
		where_clause = "WHERE rc.active = True"

	    query = query + where_clause + " ORDER BY rcr.name::date DESC"
            cur.execute(query)
	    currency_rates = cur.fetchall()
            results = []
            columns = ('baseCurrency', 'currency', 'rate', 'effectiveDate')
	    results = map(lambda x: (dict(zip(columns, x))), currency_rates)
	    res = cdtx(results, custom_root='exchangeRateList', attr_type=False)
	    #res = self.makeValidXML(res)
	    cur.close()
	    #doc = etree.XML(res)
            #DOMTree = xml.dom.minidom.parse(res)
            #collection = DOMTree.documentElement
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)


#importIMOSInvoice:
    def importFreightInvoice(self, transaction_no, invoice_date):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()
            cur = self.conn.cursor()
            query = "INSERT INTO imos_invoice_staging (transaction_no, date_invoice) VALUES ('{}', '{}') RETURNING ID".format(transaction_no, invoice_date)  
            cur.execute(query)
	    imos_inv_ids = cur.fetchall()
            results = []
            columns = ('id')
	    results = map(lambda x: (dict(zip(columns, x))), imos_inv_ids)
	    res = dtx(results, custom_root='ID', attr_type=False)
	    #res = json.dumps(results, cls=PostgresJsonEncoder)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)


