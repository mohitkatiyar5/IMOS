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
    def importFreightInvoice(self, invoice_vals):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()
            cur = self.conn.cursor()
	    print"==========invoice_vals==========", invoice_vals
	    status = invoice_vals.get('status', None)
	    transNo = invoice_vals.get('transNo', None)
	    transType = invoice_vals.get('transType', None)
	    externalRefId = invoice_vals.get('externalRefId', None)
            billExternalRef = invoice_vals.get('billExternalRef', None)
	    vendorNo = invoice_vals.get('vendorNo', None)
            vendorName = invoice_vals.get('vendorName', None)
	    vendorShortName = invoice_vals.get('vendorShortName', None)
            vendorExternalRef = invoice_vals.get('vendorExternalRef', None)
	    vendorType = invoice_vals.get('vendorType', None)
	    vendorCountryCode = invoice_vals.get('vendorCountryCode', None)
	    vendorCrossRef = invoice_vals.get('vendorCrossRef', None)
	    vendorCareOf = invoice_vals.get('vendorCareOf', None)
            vendorCareOfRef = invoice_vals.get('vendorCareOfRef', None)
	    vendorCareOfCountryCode = invoice_vals.get('vendorCareOfCountryCode', None)
            invoiceNo = invoice_vals.get('invoiceNo', None)
	    revInvoiceNo = invoice_vals.get('revInvoiceNo', None)
            purchaseOrderNo = invoice_vals.get('purchaseOrderNo', None)
	    memo = invoice_vals.get('memo', None)
	    billRemarks = invoice_vals.get('billRemarks', None)
	    approval = invoice_vals.get('approval', None)
	    paymentTermsCode = invoice_vals.get('paymentTermsCode', None)
            invoiceDate = invoice_vals.get('invoiceDate', None)
	    entryDate = invoice_vals.get('entryDate', None)
            actDate = invoice_vals.get('actDate', None)
	    dueDate = invoice_vals.get('dueDate', None)
            exchangeRateDate = invoice_vals.get('exchangeRateDate', None)
	    remarks = invoice_vals.get('remarks', None)
	    cpDate = invoice_vals.get('cpDate', None)
	    aparCode = invoice_vals.get('aparCode', None)
	    currencyAmount = invoice_vals.get('currencyAmount', None)
            currency = invoice_vals.get('currency', None)
	    exchangeRate = invoice_vals.get('exchangeRate', None)
            baseCurrencyAmount = invoice_vals.get('baseCurrencyAmount', None)
	    oprTransNo = invoice_vals.get('oprTransNo', None)
            oprBillSource = invoice_vals.get('oprBillSource', None)
	    remittanceSeq = invoice_vals.get('remittanceSeq', None)
	    remittanceCompNo = invoice_vals.get('remittanceCompNo', None)
	    remittanceAccountNo = invoice_vals.get('remittanceAccountNo', None)
	    remittanceBankName = invoice_vals.get('remittanceBankName', None)
            remittanceExternalRef = invoice_vals.get('remittanceExternalRef', None)
	    remittanceSwiftCode = invoice_vals.get('remittanceSwiftCode', None)
            remittanceFullName = invoice_vals.get('remittanceFullName', None)
	    remittanceIban = invoice_vals.get('remittanceIban', None)
            docNo = invoice_vals.get('docNo', None)
	    companyBU = invoice_vals.get('companyBU', None)
            counterpartyBU = invoice_vals.get('counterpartyBU', None)
	    paymentAccountNo = invoice_vals.get('paymentAccountNo', None)
	    paymentBank = invoice_vals.get('paymentBank', None)
	    paymentBankCode = invoice_vals.get('paymentBankCode', None)
	    lastUserId = invoice_vals.get('lastUserId', None)
            lastModifiedDate = invoice_vals.get('lastModifiedDate', None)


	    invoiceDetails = invoice_vals.get('invoiceDetails', [])
	    for inv_lines in invoiceDetails:
	        print"========inv_lines==========", inv_lines.get('transNo', None)

		transNo = inv_lines.get('transNo', None)
	        transType = inv_lines.get('transType', None)
		seqNo = inv_lines.get('seqNo', None)
		oprSeqNo = inv_lines.get('oprSeqNo', None)
		oprBillCode = inv_lines.get('oprBillCode', None)
		billSubSeq = inv_lines.get('billSubSeq', None)
		billSubCode = inv_lines.get('billSubCode', None)
		billSubSource = inv_lines.get('billSubSource', None)
		companyCode = inv_lines.get('companyCode', None)
		companyExternalRef = inv_lines.get('companyExternalRef', None)
		companyContact = inv_lines.get('companyContact', None)
		companyContactPhone = inv_lines.get('companyContactPhone', None)
		lobCode = inv_lines.get('lobCode', None)
		deptCode = inv_lines.get('deptCode', None)
		vesselCode = inv_lines.get('vesselCode', None)
		vesselName = inv_lines.get('vesselName', None)
		vesselExternalRef = inv_lines.get('vesselExternalRef', None)
		vesselCrossRef = inv_lines.get('vesselCrossRef', None)
		vesselImoNo = inv_lines.get('vesselImoNo', None)
		vesselType = inv_lines.get('vesselType', None)
		vesselGRT = inv_lines.get('vesselGRT', None)
		vendorNo = inv_lines.get('vendorNo', None)
		vendorName = inv_lines.get('vendorName', None)
		vendorShortName = inv_lines.get('vendorShortName', None)
		vendorExternalRef = inv_lines.get('vendorExternalRef', None)
		vendorCrossRef = inv_lines.get('vendorCrossRef', None)
		vendorIsInternal = inv_lines.get('vendorIsInternal', None)
		intercompanyCode = inv_lines.get('intercompanyCode', None)
		voyageNo = inv_lines.get('voyageNo', None)
		portName = inv_lines.get('portName', None)
		portNo = inv_lines.get('portNo', None)
		portUNCode = inv_lines.get('portUNCode', None)
		portCountryCode = inv_lines.get('portCountryCode', None)
		ledgerCode = inv_lines.get('ledgerCode', None)
		aparCode = inv_lines.get('aparCode', None)
		actDate = inv_lines.get('actDate', None)
		memo = inv_lines.get('memo', None)

		currencyAmount = inv_lines.get('currencyAmount', None)
		currency = inv_lines.get('currency', None)
		exchangeRate = inv_lines.get('exchangeRate', None)
		baseCurrencyAmount = inv_lines.get('baseCurrencyAmount', None)
		taxCode = inv_lines.get('taxCode', None)
		companyBrokerage = inv_lines.get('companyBrokerage', None)
		counterpartyBrokerage = inv_lines.get('counterpartyBrokerage', None)
		lastUserId = inv_lines.get('lastUserId', None)
		lastModifiedDate = inv_lines.get('lastModifiedDate', None)
		description = inv_lines.get('description', None)
		taxRate = inv_lines.get('taxRate', None)
		tradeRoute = inv_lines.get('tradeRoute', None)
		tradeRouteCode = inv_lines.get('tradeRouteCode', None)
		tradeRouteExtRef = inv_lines.get('tradeRouteExtRef', None)

		oprType = inv_lines.get('oprType', None)
		opsCoordinator = inv_lines.get('opsCoordinator', None)
		voyRef = inv_lines.get('voyRef', None)
		voyageCompanyCode = inv_lines.get('voyageCompanyCode', None)
		voyageTCICode = inv_lines.get('voyageTCICode', None)
		voyageTCOCode = inv_lines.get('voyageTCOCode', None)
		freightType = inv_lines.get('freightType', None)
		freightRate = inv_lines.get('freightRate', None)
		charterer = inv_lines.get('charterer', None)
		chartererNo = inv_lines.get('chartererNo', None)
		chartererExternalRef = inv_lines.get('chartererExternalRef', None)
		shipper = inv_lines.get('shipper', None)
		debtor = inv_lines.get('debtor', None)
		freightCollector = inv_lines.get('freightCollector', None)

		shipperNo = inv_lines.get('shipperNo', None)
		debtorNo = inv_lines.get('debtorNo', None)
		freightCollectorNo = inv_lines.get('freightCollectorNo', None)
		lumpsum = inv_lines.get('lumpsum', None)
		percentage = inv_lines.get('percentage', None)
		quantity = inv_lines.get('quantity', None)
		baseFreightAmount = inv_lines.get('baseFreightAmount', None)
		BLDate = inv_lines.get('BLDate', None)
		BLCode = inv_lines.get('BLCode', None)
		cpUnit = inv_lines.get('cpUnit', None)
		consignee = inv_lines.get('consignee', None)
		commercialId = inv_lines.get('commercialId', None)
		consigneeNo = inv_lines.get('consigneeNo', None)
		agent = inv_lines.get('agent', None)

		refBLNo = inv_lines.get('refBLNo', None)
		combineIndicator = inv_lines.get('combineIndicator', None)
		transhipIndicator = inv_lines.get('transhipIndicator', None)
		transhipSeq = inv_lines.get('transhipSeq', None)
		transhipDate = inv_lines.get('transhipDate', None)
		transhipPort = inv_lines.get('transhipPort', None)
		transhipToVessel = inv_lines.get('transhipToVessel', None)
		transhipToVoyNo = inv_lines.get('transhipToVoyNo', None)
		transhipGross = inv_lines.get('transhipGross', None)
		transhipGrossUnit = inv_lines.get('transhipGrossUnit', None)
		cargoFullName = inv_lines.get('cargoFullName', None)
		cargoGroupCode = inv_lines.get('cargoGroupCode', None)
		blQty = inv_lines.get('blQty', None)
		cargoId = inv_lines.get('cargoId', None)

		importedCargo = inv_lines.get('importedCargo', None)
		coaNo = inv_lines.get('coaNo', None)
		cargoRefContract = inv_lines.get('cargoRefContract', None)
		cargoExposureVesselNumber = inv_lines.get('cargoExposureVesselNumber', None)
		cargoName = inv_lines.get('cargoName', None)
		broker = inv_lines.get('broker', None)

		invoiceCargoInfo = inv_lines.get('invoiceCargoInfo', None)
		name = invoiceCargoInfo.get('name', None)
		broker = invoiceCargoInfo.get('broker', None)


		port = invoiceCargoInfo.get('port', None)
		cn = 0
		for port_dict in port:
		    if cn == 0:
		       name1 = port_dict.get('name1', None)
		       func1 = port_dict.get('func1', None)
		    elif cn == 1:
		       name2 = port_dict.get('name2', None)
		       func2 = port_dict.get('func2', None)

		    cn = cn + 1
		portCallSeq = inv_lines.get('portCallSeq', None)
		voyageRef = inv_lines.get('voyageRef', None)
		cargoVesselNumber = inv_lines.get('cargoVesselNumber', None)
		cargoReference = inv_lines.get('cargoReference', None)
		vcinVesselNumber = inv_lines.get('vcinVesselNumber', None)

		vcinReference = inv_lines.get('vcinReference', None)
		voyageRef = inv_lines.get('voyageRef', None)
		cargoVesselNumber = inv_lines.get('cargoVesselNumber', None)
		cargoReference = inv_lines.get('cargoReference', None)
		vcinVesselNumber = inv_lines.get('vcinVesselNumber', None)
	
		vcinVesselNumber = inv_lines.get('vcinVesselNumber', None)


		
		itinerary = inv_lines.get('itinerary', [])
	    	for itinerary_lines in itinerary:
		    iport = itinerary_lines.get('port', None)
		    iarrival = itinerary_lines.get('arrival', None)
		    ideparture = itinerary_lines.get('departure', None)
		    iportUNCode = itinerary_lines.get('portUNCode', None)
		    iportCountryCode = itinerary_lines.get('iportCountryCode', None)

		#inv_lines_dict = dict(val for val in inv_lines.iteritems())
	
	    print"=======lastModifiedDate======", lastModifiedDate

	    
	
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

#importIMOSInvoice:
    def importTCOInvoice(self, transaction_no, invoice_date):
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


