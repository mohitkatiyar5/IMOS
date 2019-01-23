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

    def validate(self, value):
	print"========value=======", value
	if value:
	   if not (isinstance(value, list) or isinstance(value, tuple) or isinstance(value, dict)):
	      value = value.encode('utf8') 
	   if isinstance(value, str):
	      value = repr(value.replace("'", "''")).replace('"', "'")      
	else:
	   return 'Null'   
	return value


#importIMOSInvoice:
    def importFreightInvoice(self, invoice_vals):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()

            cur = self.conn.cursor()
	    status = self.validate(invoice_vals.get('status', None))
	    transNo = self.validate(invoice_vals.get('transNo', None))
	    transType = self.validate(invoice_vals.get('transType', None))
	    externalRefId = self.validate(invoice_vals.get('externalRefId', None))
            billExternalRef = self.validate(invoice_vals.get('billExternalRef', None))
	    vendorNo = self.validate(invoice_vals.get('vendorNo', None))
            vendorName = self.validate(invoice_vals.get('vendorName', None))
	    vendorShortName = self.validate(invoice_vals.get('vendorShortName', None))
            vendorExternalRef = self.validate(invoice_vals.get('vendorExternalRef', None))
	    vendorType = self.validate(invoice_vals.get('vendorType', None))
	    vendorCountryCode = self.validate(invoice_vals.get('vendorCountryCode', None))
	    vendorCrossRef = self.validate(invoice_vals.get('vendorCrossRef', None))
	    vendorCareOf = self.validate(invoice_vals.get('vendorCareOf', None))
            vendorCareOfRef = self.validate(invoice_vals.get('vendorCareOfRef', None))
	    vendorCareOfCountryCode = self.validate(invoice_vals.get('vendorCareOfCountryCode', None))
            invoiceNo = self.validate(invoice_vals.get('invoiceNo', None))
	    revInvoiceNo = self.validate(invoice_vals.get('revInvoiceNo', None))
            purchaseOrderNo = self.validate(invoice_vals.get('purchaseOrderNo', None))
	    memo = self.validate(invoice_vals.get('memo', None))
	    billRemarks = self.validate(invoice_vals.get('billRemarks', None))
	    approval = self.validate(invoice_vals.get('approval', None))
	    paymentTermsCode = self.validate(invoice_vals.get('paymentTermsCode', None))
            invoiceDate = self.validate(invoice_vals.get('invoiceDate', None))
	    entryDate = self.validate(invoice_vals.get('entryDate', None))
            actDate = self.validate(invoice_vals.get('actDate', None))
	    dueDate = self.validate(invoice_vals.get('dueDate', None))
            exchangeRateDate = self.validate(invoice_vals.get('exchangeRateDate', None))
	    remarks = self.validate(invoice_vals.get('remarks', None))
	    cpDate = self.validate(invoice_vals.get('cpDate', None))
	    aparCode = self.validate(invoice_vals.get('aparCode', None))
	    currencyAmount = self.validate(invoice_vals.get('currencyAmount', None))
            currency = self.validate(invoice_vals.get('currency', None))
	    exchangeRate = self.validate(invoice_vals.get('exchangeRate', None))
            baseCurrencyAmount = self.validate(invoice_vals.get('baseCurrencyAmount', None))
	    oprTransNo = self.validate(invoice_vals.get('oprTransNo', None))
            oprBillSource = self.validate(invoice_vals.get('oprBillSource', None))
	    remittanceSeq = self.validate(invoice_vals.get('remittanceSeq', None))
	    remittanceCompNo = self.validate(invoice_vals.get('remittanceCompNo', None))
	    remittanceAccountNo = self.validate(invoice_vals.get('remittanceAccountNo', None))
	    remittanceBankName = self.validate(invoice_vals.get('remittanceBankName', None))
            remittanceExternalRef = self.validate(invoice_vals.get('remittanceExternalRef', None))
	    remittanceSwiftCode = self.validate(invoice_vals.get('remittanceSwiftCode', None))
            remittanceFullName = self.validate(invoice_vals.get('remittanceFullName', None))
	    remittanceIban = self.validate(invoice_vals.get('remittanceIban', None))
            docNo = self.validate(invoice_vals.get('docNo', None))
	    companyBU = self.validate(invoice_vals.get('companyBU', None))
            counterpartyBU = self.validate(invoice_vals.get('counterpartyBU', None))
	    paymentAccountNo = self.validate(invoice_vals.get('paymentAccountNo', None))
	    paymentBank = self.validate(invoice_vals.get('paymentBank', None))
	    paymentBankCode = self.validate(invoice_vals.get('paymentBankCode', None))
	    lastUserId = self.validate(invoice_vals.get('lastUserId', None))
            lastModifiedDate = self.validate(invoice_vals.get('lastModifiedDate', None))

	    col1 = '(status, trans_no, trans_type, external_ref_id, bill_external_ref, vendor_no, vendor_name, vendor_short_name, vendor_external_ref, vendor_type, vendor_country_code, vendor_cross_ref, vendor_careof, vendor_careof_ref, vendor_careof_country_code, invoice_no, rev_invoice_no, purchaseorder_no, memo, bill_remarks, approval, payment_terms_code, invoice_date, entry_date, act_date, exchangerate_date, remarks, apar_code, currency_amount, currency, exchangerate, base_currency_amount, opr_trans_no, opr_bill_source, remittance_seq, remittance_comp_no, remittance_account_no, remittance_bank_name, remittance_external_ref, remittance_swift_code, remittance_full_name, remittance_iban, doc_no, company_bu, counterparty_bu, payment_account_no, payment_bank, payment_bank_code, last_user_id, last_modified_date, cp_date, create_date, create_uid, write_date, write_uid)'

	    qry1 = "INSERT INTO imos_freight_invoice %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col1, status, transNo, transType, externalRefId, billExternalRef, vendorNo, vendorName, vendorShortName, vendorExternalRef, vendorType, vendorCountryCode, vendorCrossRef, vendorCareOf, vendorCareOfRef, vendorCareOfCountryCode, invoiceNo, revInvoiceNo, purchaseOrderNo, memo, billRemarks, approval, paymentTermsCode, invoiceDate, entryDate, actDate, exchangeRateDate, remarks, aparCode, currencyAmount, currency, exchangeRate, baseCurrencyAmount, oprTransNo, oprBillSource, remittanceSeq, remittanceCompNo, remittanceAccountNo, remittanceBankName, remittanceExternalRef, remittanceSwiftCode, remittanceFullName, remittanceIban, docNo, companyBU, counterpartyBU, paymentAccountNo, paymentBank, paymentBankCode, lastUserId, lastModifiedDate, cpDate) 
	     
	    cur.execute(qry1)
	    inv_id = cur.fetchone()[0]

	    invoiceDetails = self.validate(invoice_vals.get('invoiceDetails', []))
	    if not isinstance(invoiceDetails, list):
		invoiceDetails = [invoiceDetails]
	    for inv_lines in invoiceDetails:
		transNo = self.validate(inv_lines.get('transNo', None))
	        transType = self.validate(inv_lines.get('transType', None))
		seqNo = self.validate(inv_lines.get('seqNo', None))
		oprSeqNo = self.validate(inv_lines.get('oprSeqNo', None))
		oprBillCode = self.validate(inv_lines.get('oprBillCode', None))
		billSubSeq = self.validate(inv_lines.get('billSubSeq', None))
		billSubCode = self.validate(inv_lines.get('billSubCode', None))
		billSubSource = self.validate(inv_lines.get('billSubSource', None))
		companyCode = self.validate(inv_lines.get('companyCode', None))
		companyExternalRef = self.validate(inv_lines.get('companyExternalRef', None))
		companyContact = self.validate(inv_lines.get('companyContact', None))
		companyContactPhone = self.validate(inv_lines.get('companyContactPhone', None))
		lobCode = self.validate(inv_lines.get('lobCode', None))
		deptCode = self.validate(inv_lines.get('deptCode', None))
		vesselCode = self.validate(inv_lines.get('vesselCode', None))
		vesselName = self.validate(inv_lines.get('vesselName', None))
		vesselExternalRef = self.validate(inv_lines.get('vesselExternalRef', None))
		vesselCrossRef = self.validate(inv_lines.get('vesselCrossRef', None))
		vesselImoNo = self.validate(inv_lines.get('vesselImoNo', None))
		vesselType = self.validate(inv_lines.get('vesselType', None))
		vesselGRT = self.validate(inv_lines.get('vesselGRT', None))
		vendorNo = self.validate(inv_lines.get('vendorNo', None))
		vendorName = self.validate(inv_lines.get('vendorName', None))
		vendorShortName = self.validate(inv_lines.get('vendorShortName', None))
		vendorExternalRef = self.validate(inv_lines.get('vendorExternalRef', None))
		vendorCrossRef = self.validate(inv_lines.get('vendorCrossRef', None))
		vendorIsInternal = self.validate(inv_lines.get('vendorIsInternal', None))
		intercompanyCode = self.validate(inv_lines.get('intercompanyCode', None))
		voyageNo = self.validate(inv_lines.get('voyageNo', None))
		portName = self.validate(inv_lines.get('portName', None))
		portNo = self.validate(inv_lines.get('portNo', None))
		portUNCode = self.validate(inv_lines.get('portUNCode', None))
		portCountryCode = self.validate(inv_lines.get('portCountryCode', None))
		ledgerCode = self.validate(inv_lines.get('ledgerCode', None))
		aparCode = self.validate(inv_lines.get('aparCode', None))
		actDate = self.validate(inv_lines.get('actDate', None))
		memo = self.validate(inv_lines.get('memo', None))
		currencyAmount = self.validate(inv_lines.get('currencyAmount', None))
		currency = self.validate(inv_lines.get('currency', None))
		exchangeRate = self.validate(inv_lines.get('exchangeRate', None))
		baseCurrencyAmount = self.validate(inv_lines.get('baseCurrencyAmount', None))
		taxCode = self.validate(inv_lines.get('taxCode', None))
		companyBrokerage = self.validate(inv_lines.get('companyBrokerage', None))
		counterpartyBrokerage = self.validate(inv_lines.get('counterpartyBrokerage', None))
		lastUserId = self.validate(inv_lines.get('lastUserId', None))
		lastModifiedDate = self.validate(inv_lines.get('lastModifiedDate', None))
		description = self.validate(inv_lines.get('description', None))
		taxRate = self.validate(inv_lines.get('taxRate', None))
		tradeRoute = self.validate(inv_lines.get('tradeRoute', None))
		tradeRouteCode = self.validate(inv_lines.get('tradeRouteCode', None))
		tradeRouteExtRef = self.validate(inv_lines.get('tradeRouteExtRef', None))
		oprType = self.validate(inv_lines.get('oprType', None))
		opsCoordinator = self.validate(inv_lines.get('opsCoordinator', None))
		voyRef = self.validate(inv_lines.get('voyRef', None))
		voyageCompanyCode = self.validate(inv_lines.get('voyageCompanyCode', None))
		voyageTCICode = self.validate(inv_lines.get('voyageTCICode', None))
		voyageTCOCode = self.validate(inv_lines.get('voyageTCOCode', None))
		freightType = self.validate(inv_lines.get('freightType', None))
		freightRate = self.validate(inv_lines.get('freightRate', None))
		charterer = self.validate(inv_lines.get('charterer', None))
		chartererNo = self.validate(inv_lines.get('chartererNo', None))
		chartererExternalRef = self.validate(inv_lines.get('chartererExternalRef', None))
		shipper = self.validate(inv_lines.get('shipper', None))
		debtor = self.validate(inv_lines.get('debtor', None))
		freightCollector = self.validate(inv_lines.get('freightCollector', None))
		shipperNo = self.validate(inv_lines.get('shipperNo', None))
		debtorNo = self.validate(inv_lines.get('debtorNo', None))
		freightCollectorNo = self.validate(inv_lines.get('freightCollectorNo', None))
		lumpsum = self.validate(inv_lines.get('lumpsum', None))
		percentage = self.validate(inv_lines.get('percentage', None))
		quantity = self.validate(inv_lines.get('quantity', None))
		baseFreightAmount = self.validate(inv_lines.get('baseFreightAmount', None))
		BLDate = self.validate(inv_lines.get('BLDate', None))
		if not isinstance(BLDate, str):
		   BLDate = 'Null'
		BLCode = self.validate(inv_lines.get('BLCode', None))
		cpUnit = self.validate(inv_lines.get('cpUnit', None))
		consignee = self.validate(inv_lines.get('consignee', None))
		commercialId = self.validate(inv_lines.get('commercialId', None))
		consigneeNo = self.validate(inv_lines.get('consigneeNo', None))
		agent = self.validate(inv_lines.get('agent', None))
		refBLNo = self.validate(inv_lines.get('refBLNo', None))
		combineIndicator = self.validate(inv_lines.get('combineIndicator', None))
		transhipIndicator = self.validate(inv_lines.get('transhipIndicator', None))
		transhipSeq = self.validate(inv_lines.get('transhipSeq', None))
		transhipDate = self.validate(inv_lines.get('transhipDate', None))
		transhipPort = self.validate(inv_lines.get('transhipPort', None))
		transhipToVessel = self.validate(inv_lines.get('transhipToVessel', None))
		transhipToVoyNo = self.validate(inv_lines.get('transhipToVoyNo', None))
		transhipGross = self.validate(inv_lines.get('transhipGross', None))
		transhipGrossUnit = self.validate(inv_lines.get('transhipGrossUnit', None))
		cargoFullName = self.validate(inv_lines.get('cargoFullName', None))
		cargoGroupCode = self.validate(inv_lines.get('cargoGroupCode', None))
		blQty = self.validate(inv_lines.get('blQty', None))
		cargoId = self.validate(inv_lines.get('cargoId', None))
		importedCargo = self.validate(inv_lines.get('importedCargo', None))
		coaNo = self.validate(inv_lines.get('coaNo', None))
		cargoRefContract = self.validate(inv_lines.get('cargoRefContract', None))
		cargoExposureVesselNumber = self.validate(inv_lines.get('cargoExposureVesselNumber', None))
		cargoName = self.validate(inv_lines.get('cargoName', None))
		broker = self.validate(inv_lines.get('broker', None))
		invoiceCargoInfo = self.validate(inv_lines.get('invoiceCargoInfo', None))
		if not isinstance(invoiceCargoInfo, dict):
		   invoiceCargoInfo = {}	
		cname = self.validate(invoiceCargoInfo.get('name', None))
		cbroker = self.validate(invoiceCargoInfo.get('broker', None))

		port = self.validate(invoiceCargoInfo.get('port', None))
		cn = 0
		name1 = 'Null'
		func1 = 'Null'
		name2 = 'Null'
		func2 = 'Null'
		if not isinstance(port, list):
		   port = []
		for port_dict in port:
		    if cn == 0:
		       name1 = self.validate(port_dict.get('name1', None))
		       func1 = self.validate(port_dict.get('func1', None))
		    elif cn == 1:
		       name2 = self.validate(port_dict.get('name2', None))
		       func2 = self.validate(port_dict.get('func2', None))

		    cn = cn + 1
		portCallSeq = self.validate(inv_lines.get('portCallSeq', None))
		voyageRef = self.validate(inv_lines.get('voyageRef', None))
		cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		cargoReference = self.validate(inv_lines.get('cargoReference', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))

		vcinReference = self.validate(inv_lines.get('vcinReference', None))
		voyageRef = self.validate(inv_lines.get('voyageRef', None))
		cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		cargoReference = self.validate(inv_lines.get('cargoReference', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))

	        col2 = '(freight_invoice_id, trans_no, trans_type, seq_no, opr_seq_no, opr_bill_code, bill_sub_seq, bill_sub_code, bill_sub_source, company_code, company_external_ref, company_contact, company_contact_phone, lob_code, dept_code, vessel_code, vessel_name, vessel_external_ref, vessel_cross_ref, vessel_imo_no, vessel_type, vessel_grt, vendor_no, vendor_name, vendor_short_name, vendor_external_ref, vendor_cross_ref, vendor_is_internal, intercompany_code, voyage_no, port_name, port_no, port_un_code, port_country_code, ledger_code, apar_code, act_date, memo,	currency_amount, currency, exchangerate, base_currency_amount, tax_code, company_brokerage, counterparty_brokerage, last_user_id, last_modified_date, description, tax_rate, trade_route, trade_route_code, trade_route_ext_ref, opr_type, ops_coordinator, voy_ref, voyage_company_code, voyage_tci_code, voyage_tco_code, freight_type, freight_rate, charterer, charterer_no, charterer_external_ref, shipper, debtor, freight_collector, shipper_no, debtor_no, freight_collector_no, lumpsum, percentage, quantity, base_freight_amount, bl_date, bl_code,	cp_unit, consignee,  commercial_id, consignee_no, agent, ref_bl_no, combine_indicator, tranship_indicator, tranship_seq, tranship_date, tranship_port, tranship_to_vessel, tranship_to_voy_no, tranship_gross, tranship_gross_unit, cargo_full_name, cargo_group_code, bl_qty, cargold, imported_cargo, cao_no, cargo_ref_contract, cargo_exposure_vessel_number, cargo_name, broker, name1, func1, name2, func2,	port_call_seq, voyage_ref,	cargo_vessel_number, cargo_reference, vcin_vessel_number, vcin_reference, create_date, create_uid, write_date, write_uid)'

		qry2 = "INSERT INTO imos_freight_invoice_line %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col2, inv_id, transNo, transType, seqNo, oprSeqNo, oprBillCode, billSubSeq, billSubCode, billSubSource, companyCode, companyExternalRef, companyContact, companyContactPhone, lobCode, deptCode, vesselCode, vesselName, vesselExternalRef, vesselCrossRef, vesselImoNo, vesselType, vesselGRT, vendorNo, vendorName, vendorShortName, vendorExternalRef, vendorCrossRef, vendorIsInternal, intercompanyCode, voyageNo, portName, portNo, portUNCode, portCountryCode, ledgerCode, aparCode, actDate, memo, currencyAmount , currency ,exchangeRate, baseCurrencyAmount, taxCode, companyBrokerage , counterpartyBrokerage, lastUserId, lastModifiedDate, description, taxRate, tradeRoute, tradeRouteCode, tradeRouteExtRef, oprType, opsCoordinator, voyRef, voyageCompanyCode, voyageTCICode, voyageTCOCode, freightType, freightRate, charterer, chartererNo, chartererExternalRef , shipper, debtor, freightCollector, shipperNo, debtorNo, freightCollectorNo, lumpsum, percentage,  quantity, baseFreightAmount, BLDate, BLCode, cpUnit, consignee,  commercialId,  consigneeNo, agent, refBLNo, combineIndicator, transhipIndicator, transhipSeq, transhipDate, transhipPort, transhipToVessel, transhipToVoyNo, transhipGross, transhipGrossUnit, cargoFullName, cargoGroupCode, blQty, cargoId, importedCargo, coaNo, cargoRefContract, cargoExposureVesselNumber, cargoName, broker, name1, func1, name2, func2, portCallSeq, voyageRef, cargoVesselNumber, cargoReference, vcinVesselNumber, vcinReference)
		
		cur.execute(qry2)
		line_id = cur.fetchone()[0]
		
		itinerary = self.validate(inv_lines.get('itinerary', []))
		if not isinstance(itinerary, list):
			itinerary = [itinerary]
	    	for itinerary_lines in itinerary:
		    iport = self.validate(itinerary_lines.get('port', None))
		    iarrival = self.validate(itinerary_lines.get('arrival', None))
		    ideparture = self.validate(itinerary_lines.get('departure', None))
		    iportUNCode = self.validate(itinerary_lines.get('portUNCode', None))
		    iportCountryCode = self.validate(itinerary_lines.get('iportCountryCode', None))

		    qry3 = "INSERT INTO itinerary_itinerary (freight_invoice_line_id, port, arrival, departure, port_un_code, port_country_code) VALUES (%s, %s, %s, %s, %s, %s)" % (line_id, iport, iarrival, ideparture, iportUNCode, iportCountryCode)	
		    cur.execute(qry3)

            #inv_lines_dict = dict(val for val in inv_lines.iteritems())    
            #columns = ('id')
	    #results = map(lambda x: (dict(zip(columns, x))), [inv_id])
	    res = dtx([{'id': inv_id}], custom_root='ID', attr_type=False)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)


#importTCO_IMOS_Invoice:
    def importTCOInvoice(self, invoice_vals):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()

            cur = self.conn.cursor()
	    status = self.validate(invoice_vals.get('status', None))
	    transNo = self.validate(invoice_vals.get('transNo', None))
	    transType = self.validate(invoice_vals.get('transType', None))
	    externalRefId = self.validate(invoice_vals.get('externalRefId', None))
            billExternalRef = self.validate(invoice_vals.get('billExternalRef', None))
	    vendorNo = self.validate(invoice_vals.get('vendorNo', None))
            vendorName = self.validate(invoice_vals.get('vendorName', None))
	    #vendorShortName = self.validate(invoice_vals.get('vendorShortName', None))
            vendorExternalRef = self.validate(invoice_vals.get('vendorExternalRef', None))
	    vendorType = self.validate(invoice_vals.get('vendorType', None))
	    vendorCountryCode = self.validate(invoice_vals.get('vendorCountryCode', None))
	    vendorCrossRef = self.validate(invoice_vals.get('vendorCrossRef', None))
	    vendorCareOf = self.validate(invoice_vals.get('vendorCareOf', None))
            vendorCareOfRef = self.validate(invoice_vals.get('vendorCareOfRef', None))
	    vendorCareOfCountryCode = self.validate(invoice_vals.get('vendorCareOfCountryCode', None))
            invoiceNo = self.validate(invoice_vals.get('invoiceNo', None))
	    #revInvoiceNo = self.validate(invoice_vals.get('revInvoiceNo', None))
            purchaseOrderNo = self.validate(invoice_vals.get('purchaseOrderNo', None))
	    memo = self.validate(invoice_vals.get('memo', None))
	    billRemarks = self.validate(invoice_vals.get('billRemarks', None))
	    approval = self.validate(invoice_vals.get('approval', None))
	    paymentTermsCode = self.validate(invoice_vals.get('paymentTermsCode', None))
            invoiceDate = self.validate(invoice_vals.get('invoiceDate', None))
	    entryDate = self.validate(invoice_vals.get('entryDate', None))
            actDate = self.validate(invoice_vals.get('actDate', None))
	    dueDate = self.validate(invoice_vals.get('dueDate', None))
            exchangeRateDate = self.validate(invoice_vals.get('exchangeRateDate', None))
	    remarks = self.validate(invoice_vals.get('remarks', None))
	    cpDate = self.validate(invoice_vals.get('cpDate', None))
	    aparCode = self.validate(invoice_vals.get('aparCode', None))
	    currencyAmount = self.validate(invoice_vals.get('currencyAmount', None))
            currency = self.validate(invoice_vals.get('currency', None))
	    exchangeRate = self.validate(invoice_vals.get('exchangeRate', None))
            baseCurrencyAmount = self.validate(invoice_vals.get('baseCurrencyAmount', None))
	    oprTransNo = self.validate(invoice_vals.get('oprTransNo', None))
            oprBillSource = self.validate(invoice_vals.get('oprBillSource', None))
	    remittanceSeq = self.validate(invoice_vals.get('remittanceSeq', None))
	    remittanceCompNo = self.validate(invoice_vals.get('remittanceCompNo', None))
	    remittanceAccountNo = self.validate(invoice_vals.get('remittanceAccountNo', None))
	    remittanceBankName = self.validate(invoice_vals.get('remittanceBankName', None))
            #remittanceExternalRef = self.validate(invoice_vals.get('remittanceExternalRef', None))
	    
	    #remittanceSwiftCode = self.validate(invoice_vals.get('remittanceSwiftCode', None))
            #remittanceFullName = self.validate(invoice_vals.get('remittanceFullName', None))
	    #remittanceIban = self.validate(invoice_vals.get('remittanceIban', None))
            docNo = self.validate(invoice_vals.get('docNo', None))
	    companyBU = self.validate(invoice_vals.get('companyBU', None))
            counterpartyBU = self.validate(invoice_vals.get('counterpartyBU', None))
	    #paymentAccountNo = self.validate(invoice_vals.get('paymentAccountNo', None))
	    #paymentBank = self.validate(invoice_vals.get('paymentBank', None))
	    #paymentBankCode = self.validate(invoice_vals.get('paymentBankCode', None))
	    lastUserId = self.validate(invoice_vals.get('lastUserId', None))
            lastModifiedDate = self.validate(invoice_vals.get('lastModifiedDate', None))
	    dueDate = self.validate(invoice_vals.get('dueDate', None))
	    tcCode = self.validate(invoice_vals.get('tcCode', None))

	    col1 = '(status, trans_no, trans_type, external_ref_id, bill_external_ref, vendor_no, vendor_name, vendor_external_ref, vendor_type, vendor_country_code, vendor_cross_ref, vendor_careof, vendor_careof_ref, vendor_careof_country_code, invoice_no, purchaseorder_no, memo, bill_remarks, approval, payment_terms_code, invoice_date, entry_date, act_date, exchangerate_date, remarks, apar_code, currency_amount, currency, exchangerate, base_currency_amount, opr_trans_no, opr_bill_source, remittance_seq, remittance_comp_no, remittance_account_no, remittance_bank_name, doc_no, company_bu, counterparty_bu, last_user_id, last_modified_date, cp_date, due_date, tc_code, create_date, create_uid, write_date, write_uid)'

	    qry1 = "INSERT INTO imos_tco_bill_invoice %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col1, status, transNo, transType, externalRefId, billExternalRef, vendorNo, vendorName, vendorExternalRef, vendorType, vendorCountryCode, vendorCrossRef, vendorCareOf, vendorCareOfRef, vendorCareOfCountryCode, invoiceNo, purchaseOrderNo, memo, billRemarks, approval, paymentTermsCode, invoiceDate, entryDate, actDate, exchangeRateDate, remarks, aparCode, currencyAmount, currency, exchangeRate, baseCurrencyAmount, oprTransNo, oprBillSource, remittanceSeq, remittanceCompNo, remittanceAccountNo, remittanceBankName, docNo, companyBU, counterpartyBU, lastUserId, lastModifiedDate, cpDate, dueDate, tcCode) 
	     
	    cur.execute(qry1)
	    inv_id = cur.fetchone()[0]
	    print"======inv_id=======", inv_id

	    invoiceDetails = self.validate(invoice_vals.get('invoiceDetails', []))
	    if not isinstance(invoiceDetails, list):
		invoiceDetails = [invoiceDetails]
	    for inv_lines in invoiceDetails:
		transNo = self.validate(inv_lines.get('transNo', None))
	        transType = self.validate(inv_lines.get('transType', None))
		seqNo = self.validate(inv_lines.get('seqNo', None))
		oprSeqNo = self.validate(inv_lines.get('oprSeqNo', None))
		oprBillCode = self.validate(inv_lines.get('oprBillCode', None))
		billSubSeq = self.validate(inv_lines.get('billSubSeq', None))
		billSubCode = self.validate(inv_lines.get('billSubCode', None))
		billSubSource = self.validate(inv_lines.get('billSubSource', None))
		companyCode = self.validate(inv_lines.get('companyCode', None))
		companyExternalRef = self.validate(inv_lines.get('companyExternalRef', None))
		companyContact = self.validate(inv_lines.get('companyContact', None))
		companyContactPhone = self.validate(inv_lines.get('companyContactPhone', None))
		lobCode = self.validate(inv_lines.get('lobCode', None))
		deptCode = self.validate(inv_lines.get('deptCode', None))
		vesselCode = self.validate(inv_lines.get('vesselCode', None))
		vesselName = self.validate(inv_lines.get('vesselName', None))
		vesselExternalRef = self.validate(inv_lines.get('vesselExternalRef', None))
		vesselCrossRef = self.validate(inv_lines.get('vesselCrossRef', None))
		vesselImoNo = self.validate(inv_lines.get('vesselImoNo', None))
		vesselType = self.validate(inv_lines.get('vesselType', None))
		#vesselGRT = self.validate(inv_lines.get('vesselGRT', None))
		vendorNo = self.validate(inv_lines.get('vendorNo', None))
		vendorName = self.validate(inv_lines.get('vendorName', None))
		#vendorShortName = self.validate(inv_lines.get('vendorShortName', None))
		vendorExternalRef = self.validate(inv_lines.get('vendorExternalRef', None))
		vendorCrossRef = self.validate(inv_lines.get('vendorCrossRef', None))
		#vendorIsInternal = self.validate(inv_lines.get('vendorIsInternal', None))
		intercompanyCode = self.validate(inv_lines.get('intercompanyCode', None))
		voyageNo = self.validate(inv_lines.get('voyageNo', None))
		portName = self.validate(inv_lines.get('portName', None))
		portNo = self.validate(inv_lines.get('portNo', None))
		portUNCode = self.validate(inv_lines.get('portUNCode', None))
		#portCountryCode = self.validate(inv_lines.get('portCountryCode', None))
		ledgerCode = self.validate(inv_lines.get('ledgerCode', None))
		aparCode = self.validate(inv_lines.get('aparCode', None))
		actDate = self.validate(inv_lines.get('actDate', None))
		memo = self.validate(inv_lines.get('memo', None))
		currencyAmount = self.validate(inv_lines.get('currencyAmount', None))
		currency = self.validate(inv_lines.get('currency', None))
		exchangeRate = self.validate(inv_lines.get('exchangeRate', None))
		baseCurrencyAmount = self.validate(inv_lines.get('baseCurrencyAmount', None))
		taxCode = self.validate(inv_lines.get('taxCode', None))
		companyBrokerage = self.validate(inv_lines.get('companyBrokerage', None))
		counterpartyBrokerage = self.validate(inv_lines.get('counterpartyBrokerage', None))
		lastUserId = self.validate(inv_lines.get('lastUserId', None))
		lastModifiedDate = self.validate(inv_lines.get('lastModifiedDate', None))
		description = self.validate(inv_lines.get('description', None))
		taxRate = self.validate(inv_lines.get('taxRate', None))
		#tradeRoute = self.validate(inv_lines.get('tradeRoute', None))
		#tradeRouteCode = self.validate(inv_lines.get('tradeRouteCode', None))
		#tradeRouteExtRef = self.validate(inv_lines.get('tradeRouteExtRef', None))
		oprType = self.validate(inv_lines.get('oprType', None))
		opsCoordinator = self.validate(inv_lines.get('opsCoordinator', None))
		voyRef = self.validate(inv_lines.get('voyRef', None))
		voyageCompanyCode = self.validate(inv_lines.get('voyageCompanyCode', None))
		voyageTCICode = self.validate(inv_lines.get('voyageTCICode', None))
		voyageTCOCode = self.validate(inv_lines.get('voyageTCOCode', None))
		#freightType = self.validate(inv_lines.get('freightType', None))
		#freightRate = self.validate(inv_lines.get('freightRate', None))
		#charterer = self.validate(inv_lines.get('charterer', None))
		#chartererNo = self.validate(inv_lines.get('chartererNo', None))
		#chartererExternalRef = self.validate(inv_lines.get('chartererExternalRef', None))
		#shipper = self.validate(inv_lines.get('shipper', None))
		#debtor = self.validate(inv_lines.get('debtor', None))
		#freightCollector = self.validate(inv_lines.get('freightCollector', None))
		#shipperNo = self.validate(inv_lines.get('shipperNo', None))
		#debtorNo = self.validate(inv_lines.get('debtorNo', None))
		#freightCollectorNo = self.validate(inv_lines.get('freightCollectorNo', None))
		#lumpsum = self.validate(inv_lines.get('lumpsum', None))
		percentage = self.validate(inv_lines.get('percentage', None))
		quantity = self.validate(inv_lines.get('quantity', None))
		#baseFreightAmount = self.validate(inv_lines.get('baseFreightAmount', None))
		BLDate = self.validate(inv_lines.get('BLDate1', None)) #BLDate
		if not isinstance(BLDate, str):
		   BLDate = 'Null'
		BLCode = self.validate(inv_lines.get('BLCode', None))
		cpUnit = self.validate(inv_lines.get('cpUnit', None))
		consignee = self.validate(inv_lines.get('consignee', None))
		commercialId = self.validate(inv_lines.get('commercialId', None))
		consigneeNo = self.validate(inv_lines.get('consigneeNo', None))
		agent = self.validate(inv_lines.get('agent', None))
		refBLNo = self.validate(inv_lines.get('refBLNo', None))
		combineIndicator = self.validate(inv_lines.get('combineIndicator', None))
		transhipIndicator = self.validate(inv_lines.get('transhipIndicator', None))
		transhipSeq = self.validate(inv_lines.get('transhipSeq', None))
		transhipDate = self.validate(inv_lines.get('transhipDate', None))
		transhipPort = self.validate(inv_lines.get('transhipPort', None))
		transhipToVessel = self.validate(inv_lines.get('transhipToVessel', None))
		transhipToVoyNo = self.validate(inv_lines.get('transhipToVoyNo', None))
		transhipGross = self.validate(inv_lines.get('transhipGross', None))
		transhipGrossUnit = self.validate(inv_lines.get('transhipGrossUnit', None))
		cargoFullName = self.validate(inv_lines.get('cargoFullName', None))
		cargoGroupCode = self.validate(inv_lines.get('cargoGroupCode', None))
		blQty = self.validate(inv_lines.get('blQty', None))
		#cargoId = self.validate(inv_lines.get('cargoId', None))
		#importedCargo = self.validate(inv_lines.get('importedCargo', None))
		#coaNo = self.validate(inv_lines.get('coaNo', None))
		#cargoRefContract = self.validate(inv_lines.get('cargoRefContract', None))
		#cargoExposureVesselNumber = self.validate(inv_lines.get('cargoExposureVesselNumber', None))
		#cargoName = self.validate(inv_lines.get('cargoName', None))
		#broker = self.validate(inv_lines.get('broker', None))
		#invoiceCargoInfo = self.validate(inv_lines.get('invoiceCargoInfo', None))
		#cname = self.validate(invoiceCargoInfo.get('name', None))
		#cbroker = self.validate(invoiceCargoInfo.get('broker', None))

		#port = self.validate(invoiceCargoInfo.get('port', None))
		#cn = 0
		#for port_dict in port:
		 #   if cn == 0:
		  #     name1 = self.validate(port_dict.get('name1', None))
		   #    func1 = self.validate(port_dict.get('func1', None))
		    #elif cn == 1:
		     #  name2 = self.validate(port_dict.get('name2', None))
		      # func2 = self.validate(port_dict.get('func2', None))
		    #cn = cn + 1
	
		#portCallSeq = self.validate(inv_lines.get('portCallSeq', None)) 
		voyageRef = self.validate(inv_lines.get('voyageRef', None))
		#cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		#cargoReference = self.validate(inv_lines.get('cargoReference', None))

		#vcinReference = self.validate(inv_lines.get('vcinReference', None))
		cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		cargoReference = self.validate(inv_lines.get('cargoReference', None))
		#vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))
		periodStartDate = self.validate(inv_lines.get('periodEndDate', None))
		periodEndDate = self.validate(inv_lines.get('periodEndDate', None))
		tcoVesselNumber = self.validate(inv_lines.get('tcoVesselNumber', None))
		tcoReference = self.validate(inv_lines.get('tcoReference', None))

	        col2 = '(tco_invoice_id, trans_no, trans_type, seq_no, opr_seq_no, opr_bill_code, bill_sub_seq, bill_sub_code, bill_sub_source, company_code, company_external_ref, company_contact, company_contact_phone, lob_code, dept_code, vessel_code, vessel_name, vessel_external_ref, vessel_cross_ref, vessel_imo_no, vessel_type, vendor_no, vendor_name, vendor_external_ref, vendor_cross_ref, intercompany_code, voyage_no, port_name, port_no, port_un_code, ledger_code, apar_code, act_date, memo,	currency_amount, currency, exchangerate, base_currency_amount, tax_code, company_brokerage, counterparty_brokerage, last_user_id, last_modified_date, description, tax_rate, opr_type, ops_coordinator, voy_ref, voyage_company_code, voyage_tci_code, voyage_tco_code, percentage, quantity, bl_date, bl_code,	cp_unit, consignee,  commercial_id, consignee_no, agent, ref_bl_no, combine_indicator, tranship_indicator, tranship_seq, tranship_date, tranship_port, tranship_to_vessel, tranship_to_voy_no, tranship_gross, tranship_gross_unit, cargo_full_name, cargo_group_code, bl_qty, voyage_ref, period_start_date, period_end_date, tco_vessel_number, tco_reference, create_date, create_uid, write_date, write_uid)'

		qry2 = "INSERT INTO imos_tco_bill_invoice_line %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col2, inv_id, transNo, transType, seqNo, oprSeqNo, oprBillCode, billSubSeq, billSubCode, billSubSource, companyCode, companyExternalRef, companyContact, companyContactPhone, lobCode, deptCode, vesselCode, vesselName, vesselExternalRef, vesselCrossRef, vesselImoNo, vesselType, vendorNo, vendorName, vendorExternalRef, vendorCrossRef, intercompanyCode, voyageNo, portName, portNo, portUNCode, ledgerCode, aparCode, actDate, memo, currencyAmount , currency ,exchangeRate, baseCurrencyAmount, taxCode, companyBrokerage , counterpartyBrokerage, lastUserId, lastModifiedDate, description, taxRate, oprType, opsCoordinator, voyRef, voyageCompanyCode, voyageTCICode, voyageTCOCode, percentage,  quantity, BLDate, BLCode, cpUnit, consignee,  commercialId,  consigneeNo, agent, refBLNo, combineIndicator, transhipIndicator, transhipSeq, transhipDate, transhipPort, transhipToVessel, transhipToVoyNo, transhipGross, transhipGrossUnit, cargoFullName, cargoGroupCode, blQty, voyageRef, periodStartDate, periodEndDate, tcoVesselNumber, tcoReference)
		
		cur.execute(qry2)
		line_id = cur.fetchone()[0]
		
		itinerary = self.validate(inv_lines.get('itinerary', []))
		if not isinstance(itinerary, list):
			itinerary = [itinerary]
	    	for itinerary_lines in itinerary:
		    iport = self.validate(itinerary_lines.get('port', None))
		    iarrival = self.validate(itinerary_lines.get('arrival', None))
		    ideparture = self.validate(itinerary_lines.get('departure', None))
		    iportUNCode = self.validate(itinerary_lines.get('portUNCode', None))
		    #iportCountryCode = self.validate(itinerary_lines.get('iportCountryCode', None))

		    qry3 = "INSERT INTO imos_tco_bill_invoice_itinerary (tco_bill_invoice_line_id, port, arrival, departure, port_un_code) VALUES (%s, %s, %s, %s, %s)" % (line_id, iport, iarrival, ideparture, iportUNCode)	
		    cur.execute(qry3)

            #inv_lines_dict = dict(val for val in inv_lines.iteritems())    
            #columns = ('id')
	    #results = map(lambda x: (dict(zip(columns, x))), [inv_id])
	    res = dtx([{'id': inv_id}], custom_root='ID', attr_type=False)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)


#genericInvoiceImport:
    def genericInvoiceImport(self, invoice_vals, inv_type):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()

            cur = self.conn.cursor()
	    status = self.validate(invoice_vals.get('status', None))
	    transNo = self.validate(invoice_vals.get('transNo', None))
	    transType = self.validate(invoice_vals.get('transType', None))
	    externalRefId = self.validate(invoice_vals.get('externalRefId', None))
            billExternalRef = self.validate(invoice_vals.get('billExternalRef', None))
	    vendorNo = self.validate(invoice_vals.get('vendorNo', None))
            vendorName = self.validate(invoice_vals.get('vendorName', None))
	    vendorShortName = self.validate(invoice_vals.get('vendorShortName', None))
            vendorExternalRef = self.validate(invoice_vals.get('vendorExternalRef', None))
	    vendorType = self.validate(invoice_vals.get('vendorType', None))
	    vendorCountryCode = self.validate(invoice_vals.get('vendorCountryCode', None))
	    vendorCrossRef = self.validate(invoice_vals.get('vendorCrossRef', None))
	    vendorCareOf = self.validate(invoice_vals.get('vendorCareOf', None))
            vendorCareOfRef = self.validate(invoice_vals.get('vendorCareOfRef', None))
	    vendorCareOfCountryCode = self.validate(invoice_vals.get('vendorCareOfCountryCode', None))
            invoiceNo = self.validate(invoice_vals.get('invoiceNo', None))
	    revInvoiceNo = self.validate(invoice_vals.get('revInvoiceNo', None))
            purchaseOrderNo = self.validate(invoice_vals.get('purchaseOrderNo', None))
	    memo = self.validate(invoice_vals.get('memo', None))
	    billRemarks = self.validate(invoice_vals.get('billRemarks', None))
	    approval = self.validate(invoice_vals.get('approval', None))
	    paymentTermsCode = self.validate(invoice_vals.get('paymentTermsCode', None))
            invoiceDate = self.validate(invoice_vals.get('invoiceDate', None))
	    entryDate = self.validate(invoice_vals.get('entryDate', None))
            actDate = self.validate(invoice_vals.get('actDate', None))
	    dueDate = self.validate(invoice_vals.get('dueDate', None))
            exchangeRateDate = self.validate(invoice_vals.get('exchangeRateDate', None))
	    remarks = self.validate(invoice_vals.get('remarks', None))
	    cpDate = self.validate(invoice_vals.get('cpDate', None))
	    aparCode = self.validate(invoice_vals.get('aparCode', None))
	    currencyAmount = self.validate(invoice_vals.get('currencyAmount', None))
            currency = self.validate(invoice_vals.get('currency', None))
	    exchangeRate = self.validate(invoice_vals.get('exchangeRate', None))
            baseCurrencyAmount = self.validate(invoice_vals.get('baseCurrencyAmount', None))
	    oprTransNo = self.validate(invoice_vals.get('oprTransNo', None))
            oprBillSource = self.validate(invoice_vals.get('oprBillSource', None))
	    remittanceSeq = self.validate(invoice_vals.get('remittanceSeq', None))
	    remittanceCompNo = self.validate(invoice_vals.get('remittanceCompNo', None))
	    remittanceAccountNo = self.validate(invoice_vals.get('remittanceAccountNo', None))
	    remittanceBankName = self.validate(invoice_vals.get('remittanceBankName', None))
            remittanceExternalRef = self.validate(invoice_vals.get('remittanceExternalRef', None))
	    remittanceSwiftCode = self.validate(invoice_vals.get('remittanceSwiftCode', None))
            remittanceFullName = self.validate(invoice_vals.get('remittanceFullName', None))
	    remittanceIban = self.validate(invoice_vals.get('remittanceIban', None))
            docNo = self.validate(invoice_vals.get('docNo', None))
	    companyBU = self.validate(invoice_vals.get('companyBU', None))
            counterpartyBU = self.validate(invoice_vals.get('counterpartyBU', None))
	    paymentAccountNo = self.validate(invoice_vals.get('paymentAccountNo', None))
	    paymentBank = self.validate(invoice_vals.get('paymentBank', None))
	    paymentBankCode = self.validate(invoice_vals.get('paymentBankCode', None))
	    lastUserId = self.validate(invoice_vals.get('lastUserId', None))
            lastModifiedDate = self.validate(invoice_vals.get('lastModifiedDate', None))
	    dueDate = self.validate(invoice_vals.get('dueDate', None))

	    col1 = '(status, trans_no, trans_type, external_ref_id, bill_external_ref, vendor_no, vendor_name, vendor_short_name, vendor_external_ref, vendor_type, vendor_country_code, vendor_cross_ref, vendor_careof, vendor_careof_ref, vendor_careof_country_code, invoice_no, rev_invoice_no, purchaseorder_no, memo, bill_remarks, approval, payment_terms_code, invoice_date, entry_date, act_date, exchangerate_date, remarks, apar_code, currency_amount, currency, exchangerate, base_currency_amount, opr_trans_no, opr_bill_source, remittance_seq, remittance_comp_no, remittance_account_no, remittance_bank_name, remittance_external_ref, remittance_swift_code, remittance_full_name, remittance_iban, doc_no, company_bu, counterparty_bu, payment_account_no, payment_bank, payment_bank_code, last_user_id, last_modified_date, cp_date, due_date, create_date, create_uid, write_date, write_uid)'

	    qry1 = "INSERT INTO imos_invoice_staging %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col1, status, transNo, transType, externalRefId, billExternalRef, vendorNo, vendorName, vendorShortName, vendorExternalRef, vendorType, vendorCountryCode, vendorCrossRef, vendorCareOf, vendorCareOfRef, vendorCareOfCountryCode, invoiceNo, revInvoiceNo, purchaseOrderNo, memo, billRemarks, approval, paymentTermsCode, invoiceDate, entryDate, actDate, exchangeRateDate, remarks, aparCode, currencyAmount, currency, exchangeRate, baseCurrencyAmount, oprTransNo, oprBillSource, remittanceSeq, remittanceCompNo, remittanceAccountNo, remittanceBankName, remittanceExternalRef, remittanceSwiftCode, remittanceFullName, remittanceIban, docNo, companyBU, counterpartyBU, paymentAccountNo, paymentBank, paymentBankCode, lastUserId, lastModifiedDate, cpDate, dueDate) 
	     
	    cur.execute(qry1)
	    inv_id = cur.fetchone()[0]

	    invoiceDetails = self.validate(invoice_vals.get('invoiceDetails', []))
	    if not isinstance(invoiceDetails, list):
		invoiceDetails = [invoiceDetails]
	    for inv_lines in invoiceDetails:
		transNo = self.validate(inv_lines.get('transNo', None))
	        transType = self.validate(inv_lines.get('transType', None))
		seqNo = self.validate(inv_lines.get('seqNo', None))
		oprSeqNo = self.validate(inv_lines.get('oprSeqNo', None))
		oprBillCode = self.validate(inv_lines.get('oprBillCode', None))
		billSubSeq = self.validate(inv_lines.get('billSubSeq', None))
		billSubCode = self.validate(inv_lines.get('billSubCode', None))
		billSubSource = self.validate(inv_lines.get('billSubSource', None))
		companyCode = self.validate(inv_lines.get('companyCode', None))
		companyExternalRef = self.validate(inv_lines.get('companyExternalRef', None))
		companyContact = self.validate(inv_lines.get('companyContact', None))
		companyContactPhone = self.validate(inv_lines.get('companyContactPhone', None))
		lobCode = self.validate(inv_lines.get('lobCode', None))
		deptCode = self.validate(inv_lines.get('deptCode', None))
		vesselCode = self.validate(inv_lines.get('vesselCode', None))
		vesselName = self.validate(inv_lines.get('vesselName', None))
		vesselExternalRef = self.validate(inv_lines.get('vesselExternalRef', None))
		vesselCrossRef = self.validate(inv_lines.get('vesselCrossRef', None))
		vesselImoNo = self.validate(inv_lines.get('vesselImoNo', None))
		vesselType = self.validate(inv_lines.get('vesselType', None))
		vesselGRT = self.validate(inv_lines.get('vesselGRT', None))
		vendorNo = self.validate(inv_lines.get('vendorNo', None))
		vendorName = self.validate(inv_lines.get('vendorName', None))
		vendorShortName = self.validate(inv_lines.get('vendorShortName', None))
		vendorExternalRef = self.validate(inv_lines.get('vendorExternalRef', None))
		vendorCrossRef = self.validate(inv_lines.get('vendorCrossRef', None))
		vendorIsInternal = self.validate(inv_lines.get('vendorIsInternal', None))
		intercompanyCode = self.validate(inv_lines.get('intercompanyCode', None))
		voyageNo = self.validate(inv_lines.get('voyageNo', None))
		portName = self.validate(inv_lines.get('portName', None))
		portNo = self.validate(inv_lines.get('portNo', None))
		portUNCode = self.validate(inv_lines.get('portUNCode', None))
		portCountryCode = self.validate(inv_lines.get('portCountryCode', None))
		ledgerCode = self.validate(inv_lines.get('ledgerCode', None))
		aparCode = self.validate(inv_lines.get('aparCode', None))
		actDate = self.validate(inv_lines.get('actDate', None))
		memo = self.validate(inv_lines.get('memo', None))
		currencyAmount = self.validate(inv_lines.get('currencyAmount', None))
		currency = self.validate(inv_lines.get('currency', None))
		exchangeRate = self.validate(inv_lines.get('exchangeRate', None))
		baseCurrencyAmount = self.validate(inv_lines.get('baseCurrencyAmount', None))
		taxCode = self.validate(inv_lines.get('taxCode', None))
		companyBrokerage = self.validate(inv_lines.get('companyBrokerage', None))
		counterpartyBrokerage = self.validate(inv_lines.get('counterpartyBrokerage', None))
		lastUserId = self.validate(inv_lines.get('lastUserId', None))
		lastModifiedDate = self.validate(inv_lines.get('lastModifiedDate', None))
		description = self.validate(inv_lines.get('description', None))
		taxRate = self.validate(inv_lines.get('taxRate', None))
		tradeRoute = self.validate(inv_lines.get('tradeRoute', None))
		tradeRouteCode = self.validate(inv_lines.get('tradeRouteCode', None))
		tradeRouteExtRef = self.validate(inv_lines.get('tradeRouteExtRef', None))
		oprType = self.validate(inv_lines.get('oprType', None))
		opsCoordinator = self.validate(inv_lines.get('opsCoordinator', None))
		voyRef = self.validate(inv_lines.get('voyRef', None))
		voyageCompanyCode = self.validate(inv_lines.get('voyageCompanyCode', None))
		voyageTCICode = self.validate(inv_lines.get('voyageTCICode', None))
		voyageTCOCode = self.validate(inv_lines.get('voyageTCOCode', None))
		freightType = self.validate(inv_lines.get('freightType', None))
		freightRate = self.validate(inv_lines.get('freightRate', None))
		charterer = self.validate(inv_lines.get('charterer', None))
		chartererNo = self.validate(inv_lines.get('chartererNo', None))
		chartererExternalRef = self.validate(inv_lines.get('chartererExternalRef', None))
		shipper = self.validate(inv_lines.get('shipper', None))
		debtor = self.validate(inv_lines.get('debtor', None))
		freightCollector = self.validate(inv_lines.get('freightCollector', None))
		shipperNo = self.validate(inv_lines.get('shipperNo', None))
		debtorNo = self.validate(inv_lines.get('debtorNo', None))
		freightCollectorNo = self.validate(inv_lines.get('freightCollectorNo', None))
		lumpsum = self.validate(inv_lines.get('lumpsum', None))
		percentage = self.validate(inv_lines.get('percentage', None))
		quantity = self.validate(inv_lines.get('quantity', None))
		baseFreightAmount = self.validate(inv_lines.get('baseFreightAmount', None))
		BLDate = self.validate(inv_lines.get('BLDate', None))
		if not isinstance(BLDate, str):
		   BLDate = 'Null'
		BLCode = self.validate(inv_lines.get('BLCode', None))
		cpUnit = self.validate(inv_lines.get('cpUnit', None))
		consignee = self.validate(inv_lines.get('consignee', None))
		commercialId = self.validate(inv_lines.get('commercialId', None))
		consigneeNo = self.validate(inv_lines.get('consigneeNo', None))
		agent = self.validate(inv_lines.get('agent', None))
		refBLNo = self.validate(inv_lines.get('refBLNo', None))
		combineIndicator = self.validate(inv_lines.get('combineIndicator', None))
		transhipIndicator = self.validate(inv_lines.get('transhipIndicator', None))
		transhipSeq = self.validate(inv_lines.get('transhipSeq', None))
		transhipDate = self.validate(inv_lines.get('transhipDate', None))
		transhipPort = self.validate(inv_lines.get('transhipPort', None))
		transhipToVessel = self.validate(inv_lines.get('transhipToVessel', None))
		transhipToVoyNo = self.validate(inv_lines.get('transhipToVoyNo', None))
		transhipGross = self.validate(inv_lines.get('transhipGross', None))
		transhipGrossUnit = self.validate(inv_lines.get('transhipGrossUnit', None))
		cargoFullName = self.validate(inv_lines.get('cargoFullName', None))
		cargoGroupCode = self.validate(inv_lines.get('cargoGroupCode', None))
		blQty = self.validate(inv_lines.get('blQty', None))
		cargoId = self.validate(inv_lines.get('cargoId', None))
		importedCargo = self.validate(inv_lines.get('importedCargo', None))
		coaNo = self.validate(inv_lines.get('coaNo', None))
		cargoRefContract = self.validate(inv_lines.get('cargoRefContract', None))
		cargoExposureVesselNumber = self.validate(inv_lines.get('cargoExposureVesselNumber', None))
		cargoName = self.validate(inv_lines.get('cargoName', None))
		broker = self.validate(inv_lines.get('broker', None))
		invoiceCargoInfo = self.validate(inv_lines.get('invoiceCargoInfo', {}))
		if not isinstance(invoiceCargoInfo, dict):
		   invoiceCargoInfo = {}		
		cname = self.validate(invoiceCargoInfo.get('name', None))
		cbroker = self.validate(invoiceCargoInfo.get('broker', None))

		port = self.validate(invoiceCargoInfo.get('port', []))
		cn = 0
	        name1 = 'Null'
		func1 = 'Null'
		name2 = 'Null'
		func2 = 'Null'
		if not isinstance(port, list):
		   port = []
		for port_dict in port:
		    if cn == 0:
		       name1 = self.validate(port_dict.get('name1', None))
		       func1 = self.validate(port_dict.get('func1', None))
		    elif cn == 1:
		       name2 = self.validate(port_dict.get('name2', None))
		       func2 = self.validate(port_dict.get('func2', None))

		    cn = cn + 1
		portCallSeq = self.validate(inv_lines.get('portCallSeq', None))
		voyageRef = self.validate(inv_lines.get('voyageRef', None))
		cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		cargoReference = self.validate(inv_lines.get('cargoReference', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))

		vcinReference = self.validate(inv_lines.get('vcinReference', None))
		voyageRef = self.validate(inv_lines.get('voyageRef', None))
		cargoVesselNumber = self.validate(inv_lines.get('cargoVesselNumber', None))
		cargoReference = self.validate(inv_lines.get('cargoReference', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))
		vcinVesselNumber = self.validate(inv_lines.get('vcinVesselNumber', None))
		periodStartDate = self.validate(inv_lines.get('periodEndDate', None))
		periodEndDate = self.validate(inv_lines.get('periodEndDate', None))

	        col2 = '(staging_invoice_id, trans_no, trans_type, seq_no, opr_seq_no, opr_bill_code, bill_sub_seq, bill_sub_code, bill_sub_source, company_code, company_external_ref, company_contact, company_contact_phone, lob_code, dept_code, vessel_code, vessel_name, vessel_external_ref, vessel_cross_ref, vessel_imo_no, vessel_type, vessel_grt, vendor_no, vendor_name, vendor_short_name, vendor_external_ref, vendor_cross_ref, vendor_is_internal, intercompany_code, voyage_no, port_name, port_no, port_un_code, port_country_code, ledger_code, apar_code, act_date, memo,	currency_amount, currency, exchangerate, base_currency_amount, tax_code, company_brokerage, counterparty_brokerage, last_user_id, last_modified_date, description, tax_rate, trade_route, trade_route_code, trade_route_ext_ref, opr_type, ops_coordinator, voy_ref, voyage_company_code, voyage_tci_code, voyage_tco_code, freight_type, freight_rate, charterer, charterer_no, charterer_external_ref, shipper, debtor, freight_collector, shipper_no, debtor_no, freight_collector_no, lumpsum, percentage, quantity, base_freight_amount, bl_date, bl_code,	cp_unit, consignee,  commercial_id, consignee_no, agent, ref_bl_no, combine_indicator, tranship_indicator, tranship_seq, tranship_date, tranship_port, tranship_to_vessel, tranship_to_voy_no, tranship_gross, tranship_gross_unit, cargo_full_name, cargo_group_code, bl_qty, cargold, imported_cargo, cao_no, cargo_ref_contract, cargo_exposure_vessel_number, cargo_name, broker, name1, func1, name2, func2,	port_call_seq, voyage_ref,	cargo_vessel_number, cargo_reference, vcin_vessel_number, vcin_reference, period_start_date, period_end_date, create_date, create_uid, write_date, write_uid)'

		qry2 = "INSERT INTO imos_staging_invoice_line %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col2, inv_id, transNo, transType, seqNo, oprSeqNo, oprBillCode, billSubSeq, billSubCode, billSubSource, companyCode, companyExternalRef, companyContact, companyContactPhone, lobCode, deptCode, vesselCode, vesselName, vesselExternalRef, vesselCrossRef, vesselImoNo, vesselType, vesselGRT, vendorNo, vendorName, vendorShortName, vendorExternalRef, vendorCrossRef, vendorIsInternal, intercompanyCode, voyageNo, portName, portNo, portUNCode, portCountryCode, ledgerCode, aparCode, actDate, memo, currencyAmount , currency ,exchangeRate, baseCurrencyAmount, taxCode, companyBrokerage , counterpartyBrokerage, lastUserId, lastModifiedDate, description, taxRate, tradeRoute, tradeRouteCode, tradeRouteExtRef, oprType, opsCoordinator, voyRef, voyageCompanyCode, voyageTCICode, voyageTCOCode, freightType, freightRate, charterer, chartererNo, chartererExternalRef , shipper, debtor, freightCollector, shipperNo, debtorNo, freightCollectorNo, lumpsum, percentage,  quantity, baseFreightAmount, BLDate, BLCode, cpUnit, consignee,  commercialId,  consigneeNo, agent, refBLNo, combineIndicator, transhipIndicator, transhipSeq, transhipDate, transhipPort, transhipToVessel, transhipToVoyNo, transhipGross, transhipGrossUnit, cargoFullName, cargoGroupCode, blQty, cargoId, importedCargo, coaNo, cargoRefContract, cargoExposureVesselNumber, cargoName, broker, name1, func1, name2, func2, portCallSeq, voyageRef, cargoVesselNumber, cargoReference, vcinVesselNumber, vcinReference, periodStartDate, periodEndDate)
		
		cur.execute(qry2)
		line_id = cur.fetchone()[0]
		
		itinerary = self.validate(inv_lines.get('itinerary', []))
		if not isinstance(itinerary, list):
			itinerary = [itinerary]
	    	for itinerary_lines in itinerary:
		    iport = self.validate(itinerary_lines.get('port', None))
		    iarrival = self.validate(itinerary_lines.get('arrival', None))
		    ideparture = self.validate(itinerary_lines.get('departure', None))
		    iportUNCode = self.validate(itinerary_lines.get('portUNCode', None))
		    iportCountryCode = self.validate(itinerary_lines.get('iportCountryCode', None))

		    qry3 = "INSERT INTO imos_staging_invoice_itinerary (staging_invoice_line_id, port, arrival, departure, port_un_code, port_country_code) VALUES (%s, %s, %s, %s, %s, %s)" % (line_id, iport, iarrival, ideparture, iportUNCode, iportCountryCode)	
		    cur.execute(qry3)

            #inv_lines_dict = dict(val for val in inv_lines.iteritems())    
            #columns = ('id')
	    #results = map(lambda x: (dict(zip(columns, x))), [inv_id])
	    res = dtx([{'id': inv_id}], custom_root='ID', attr_type=False)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)


#-----------------------------------------------------------------------------------------------------
#genericPaymentImport:
    def genericPaymentImport(self, invoice_vals, pay_type):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()

            cur = self.conn.cursor()
	    invoiceTransNo = self.validate(invoice_vals.get('invoiceTransNo', None))
	    entryDate = self.validate(invoice_vals.get('entryDate', None))
	    actDate = self.validate(invoice_vals.get('actDate', None))
	    payMode = self.validate(invoice_vals.get('payMode', None))
	    bankCode = self.validate(invoice_vals.get('bankCode', None))
            currencyAmount = self.validate(invoice_vals.get('currencyAmount', None))
	    currency = self.validate(invoice_vals.get('currency', None))
            baseCurrencyAmount = self.validate(invoice_vals.get('baseCurrencyAmount', None))
            

	    col1 = '(invoice_trans_no, entry_date, act_date, pay_mode, bank_code, currency_amount, currency, base_currency_amount, create_date, create_uid, write_date, write_uid)'

	    qry1 = "INSERT INTO imos_staging_payment %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col1, invoiceTransNo, entryDate, actDate, payMode, bankCode, currencyAmount, currency, baseCurrencyAmount) 
	     
	    cur.execute(qry1)
	    payment_id = cur.fetchone()[0]

            #inv_lines_dict = dict(val for val in inv_lines.iteritems())    
            #columns = ('id')
	    #results = map(lambda x: (dict(zip(columns, x))), [inv_id])
	    res = dtx([{'id': payment_id}], custom_root='ID', attr_type=False)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)



#genericVoyageImport:
    def genericVoyageImport(self, voyage_vals):
        try:
	    if(not self.conn):
                self.ConnectToDatabase()

            cur = self.conn.cursor()
	    vesselCode = self.validate(invoice_vals.get('vesselCode', None))
	    vesselName = self.validate(invoice_vals.get('vesselName', None))
	    vesselType = self.validate(invoice_vals.get('vesselType', None))
	    vesselTypeCode = self.validate(invoice_vals.get('vesselTypeCode', None))
            vesselFlag = self.validate(invoice_vals.get('vesselFlag', None))
	    vesselDWT = self.validate(invoice_vals.get('vesselDWT', None))
            vesselGRT = self.validate(invoice_vals.get('vesselGRT', None))
	    vesselNRT = self.validate(invoice_vals.get('vesselNRT', None))
            vesselYearBuilt = self.validate(invoice_vals.get('vesselYearBuilt', None))
	    vesselNumberOfHolds = self.validate(invoice_vals.get('vesselNumberOfHolds', None))
	    vesselNumberOfHatches = self.validate(invoice_vals.get('vesselNumberOfHatches', None))
	    vesselSatPhoneNumA = self.validate(invoice_vals.get('vesselSatPhoneNumA', None))
	    vesselSatPhoneNumB = self.validate(invoice_vals.get('vesselSatPhoneNumB', None))
            vesselSatPhoneNumC = self.validate(invoice_vals.get('vesselSatPhoneNumC', None))
	    vesselCelularNum = self.validate(invoice_vals.get('vesselCelularNum', None))
            vesselFax = self.validate(invoice_vals.get('vesselFax', None))
	    vesselTelex = self.validate(invoice_vals.get('vesselTelex', None))
            vesselEmail = self.validate(invoice_vals.get('vesselEmail', None))
	    vesselCallLetters = self.validate(invoice_vals.get('vesselCallLetters', None))
	    vesselOwner = self.validate(invoice_vals.get('vesselOwner', None))
	    vesselOperator = self.validate(invoice_vals.get('vesselOperator', None))
	    vesselPniClub = self.validate(invoice_vals.get('vesselPniClub', None))
            vesselExternalRef = self.validate(invoice_vals.get('vesselExternalRef', None))
	    vesselImoNumber = self.validate(invoice_vals.get('vesselImoNumber', None))
            lastTCIVoy = self.validate(invoice_vals.get('lastTCIVoy', None))
	    latestTCIVoyNo = self.validate(invoice_vals.get('latestTCIVoyNo', None))
            voyageNo = self.validate(invoice_vals.get('voyageNo', None))
	    commenceDateTime = self.validate(invoice_vals.get('commenceDateTime', None))
	    completeDateTime = self.validate(invoice_vals.get('completeDateTime', None))
	    voyageStatus = self.validate(invoice_vals.get('voyageStatus', None))
	    oprType = self.validate(invoice_vals.get('oprType', None))
            fixtureId = self.validate(invoice_vals.get('fixtureId', None))
	    fixtureDate = self.validate(invoice_vals.get('fixtureDate', None))
            chaCoordinator = self.validate(invoice_vals.get('chaCoordinator', None))
	    oprCoordinator = self.validate(invoice_vals.get('oprCoordinator', None))
            controller = self.validate(invoice_vals.get('controller', None))
	    fdManager = self.validate(invoice_vals.get('fdManager', None))
	    financeCoordinator = self.validate(invoice_vals.get('financeCoordinator', None))
	    prevStatus = self.validate(invoice_vals.get('prevStatus', None))
	    tcoFixCode = self.validate(invoice_vals.get('tcoFixCode', None))
            tciFixCode = self.validate(invoice_vals.get('tciFixCode', None))
	    externalRef = self.validate(invoice_vals.get('externalRef', None))
            voyRef = self.validate(invoice_vals.get('voyRef', None))
	    lob = self.validate(invoice_vals.get('lob', None))
            lobFull = self.validate(invoice_vals.get('lobFull', None))
	    company = self.validate(invoice_vals.get('company', None))
            companyFull = self.validate(invoice_vals.get('companyFull', None))
	    counterparty = self.validate(invoice_vals.get('counterparty', None))
	    counterpartyFull = self.validate(invoice_vals.get('counterpartyFull', None))
	    cpDate = self.validate(invoice_vals.get('cpDate', None))
	    entryDate = self.validate(invoice_vals.get('entryDate', None))
            lastUserId = self.validate(invoice_vals.get('lastUserId', None))
	    lastModifiedDate = self.validate(invoice_vals.get('lastModifiedDate', None))

	    col1 = '(status, trans_no, trans_type, external_ref_id, bill_external_ref, vendor_no, vendor_name, vendor_short_name, vendor_external_ref, vendor_type, vendor_country_code, vendor_cross_ref, vendor_careof, vendor_careof_ref, vendor_careof_country_code, invoice_no, rev_invoice_no, purchaseorder_no, memo, bill_remarks, approval, payment_terms_code, invoice_date, entry_date, act_date, exchangerate_date, remarks, apar_code, currency_amount, currency, exchangerate, base_currency_amount, opr_trans_no, opr_bill_source, remittance_seq, remittance_comp_no, remittance_account_no, remittance_bank_name, remittance_external_ref, remittance_swift_code, remittance_full_name, remittance_iban, doc_no, company_bu, counterparty_bu, payment_account_no, payment_bank, payment_bank_code, last_user_id, last_modified_date, cp_date, due_date, create_date, create_uid, write_date, write_uid)'

	    qry1 = "INSERT INTO imos_invoice_staging %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col1, vesselCode, vesselName, vesselType, vesselTypeCode, vesselFlag, vesselDWT, vesselGRT,vesselNRT, vesselYearBuilt ,vesselNumberOfHolds,vesselNumberOfHatches, vesselSatPhoneNumA, vesselSatPhoneNumB, vesselSatPhoneNumC, vesselCelularNum, vesselFax, vesselTelex, vesselEmail, vesselCallLetters, vesselOwner, vesselOperator, vesselPniClub, vesselExternalRef, vesselImoNumber, lastTCIVoy, latestTCIVoyNo, voyageNo, commenceDateTime, completeDateTime, voyageStatus, oprType, fixtureId, fixtureDate, chaCoordinator, oprCoordinator, controller, fdManager, financeCoordinator, prevStatus, tcoFixCode, tciFixCode, externalRef, voyRef, lob, lobFull, company, companyFull, counterparty, counterpartyFull, cpDate, entryDate, lastUserId, lastModifiedDate) 
	     
	    cur.execute(qry1)
	    voyage_id = cur.fetchone()[0]

	    userProperties = self.validate(invoice_vals.get('userProperties', {}))
	    if not isinstance(userProperties, dict):
		userProperties = {}
	    userProperty = self.validate(userProperties.get('userProperty', []))
	    if not isinstance(userProperty, list):
		userProperty = [userProperty]
	    for upr in userProperty:
		fieldName = self.validate(upr.get('fieldName', None))
	        fieldID = self.validate(upr.get('fieldID', None))
		

	        col2 = '(staging_voyage_id, fieldName, fieldID, create_date, create_uid, write_date, write_uid)'

		qry2 = "INSERT INTO imos_staging_invoice_line %s VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC', 1, NOW() AT TIME ZONE 'UTC', 1) RETURNING ID" % (col2, fieldName, fieldID)
		
		cur.execute(qry2)
		line_id = cur.fetchone()[0]
		
	    portcall = self.validate(inv_lines.get('portcall', []))
	    if not isinstance(portcall, list):
	       portcall = [portcall]
	    for pc in portcall:
		seq = self.validate(pc.get('seq', None))
		function = self.validate(pc.get('function', None))
		status = self.validate(pc.get('status', None))
		portNo = self.validate(pc.get('portNo', None))
		portName = self.validate(pc.get('portName', None))

		portcallID = self.validate(pc.get('portcallID', None))
		portCountryCode = self.validate(pc.get('portCountryCode', None))
		portExternalRef = self.validate(pc.get('portExternalRef', None))
		portArea = self.validate(pc.get('portArea', None))
		portRegionCode = self.validate(pc.get('portRegionCode', None))

		portRegion = self.validate(pc.get('portRegion', None))
		portOcean = self.validate(pc.get('portOcean', None))
		defaultLocationRef = self.validate(pc.get('defaultLocationRef', None))
		ETA = self.validate(pc.get('ETA', None))
		ETALocal = self.validate(pc.get('ETALocal', None))

		cargoHandling = self.validate(pc.get('cargoHandling', {}))
		if not isinstance(cargoHandling, dict):
		   cargoHandling = {}	
		
		cargoHandlingLine = self.validate(cargoHandling.get('cargoHandlingLine', []))
		if not isinstance(cargoHandlingLine, list):
		   cargoHandlingLine = [cargoHandlingLine]
		for chl in cargoHandlingLine:
		    fixtureNo = self.validate(chl.get('fixtureNo', None))
		    vfixcarSeq = self.validate(chl.get('vfixcarSeq', None))
		    cargoId = self.validate(chl.get('cargoId', None))
		    cargoGrade = self.validate(chl.get('cargoGrade', None))
		    commercialId = self.validate(chl.get('commercialId', None))
		    portcarSeq = self.validate(chl.get('portcarSeq', None))
		    cpUnit = self.validate(chl.get('cpUnit', None))
		    cpQty = self.validate(chl.get('cpQty', None))
		    cpRate = self.validate(chl.get('cpRate', None))
		    cpRateUnit = self.validate(chl.get('cpRateUnit', None))
		    blDate = self.validate(chl.get('blDate', None))
		    if not isinstance(blDate, str):
			blDate = 'Null'		    
		    blQty = self.validate(chl.get('blQty', None))
		    blCode = self.validate(chl.get('blCode', None))
		    itinSeq = self.validate(chl.get('itinSeq', None))
		    function = self.validate(chl.get('function', None))
		    hasTsCargo = self.validate(chl.get('hasTsCargo', None))
		    berth = self.validate(chl.get('berth', None))
		    portExp = self.validate(chl.get('portExp', None))
		    portExpBase = self.validate(chl.get('portExpBase', None))
		    portExpCurr = self.validate(chl.get('portExpCurr', None))

		    qry3 = "INSERT INTO imos_staging_invoice_itinerary (staging_invoice_line_id, port, arrival, departure, port_un_code, port_country_code) VALUES (%s, %s, %s, %s, %s, %s)" % (line_id, iport, iarrival, ideparture, iportUNCode, iportCountryCode)	
		    cur.execute(qry3)

		contracts = self.validate(inv_lines.get('contracts', {}))
	        if not isinstance(contracts, dict):
	           contracts = {}

		contract = self.validate(contracts.get('contract', []))
		if not isinstance(contract, list):
	           contract = [contract]
	        for con in contract:
		    contractType = self.validate(con.get('contractType', None))
		    cargoID = self.validate(con.get('cargoID', None))
		    coaNo = self.validate(con.get('coaNo', None))
		    if not isinstance(coaNo, str):
			coaNo = 'Null'
		    voyageTCOCode = self.validate(con.get('voyageTCOCode', None))
		    if not isinstance(voyageTCOCode, str):
			voyageTCOCode = 'Null'
			
		    segment = self.validate(con.get('segment', None))
		    description = self.validate(con.get('description', None))
		    charterer = self.validate(con.get('charterer', None))
		    pool = self.validate(con.get('pool', None))
		
		    lob = self.validate(con.get('lob', None))
		    tradeArea = self.validate(con.get('tradeArea', None))
		    tradeAreaCode = self.validate(con.get('tradeAreaCode', None))
		    tradeAreaExternalRef = self.validate(con.get('tradeAreaExternalRef', None))

		    lob = self.validate(con.get('lob', None))
		    tradeArea = self.validate(con.get('tradeArea', None))
		    tradeAreaCode = self.validate(con.get('tradeAreaCode', None))
		    tradeAreaExternalRef = self.validate(con.get('tradeAreaExternalRef', None))

		portcallID = self.validate(pc.get('portcallID', None))
		portCountryCode = self.validate(pc.get('portCountryCode', None))
		portExternalRef = self.validate(pc.get('portExternalRef', None))
		portArea = self.validate(pc.get('portArea', None))
		portRegionCode = self.validate(pc.get('portRegionCode', None))

		portRegion = self.validate(pc.get('portRegion', None))
		portOcean = self.validate(pc.get('portOcean', None))
		defaultLocationRef = self.validate(pc.get('defaultLocationRef', None))
		ETA = self.validate(pc.get('ETA', None))
		ETALocal = self.validate(pc.get('ETALocal', None))

            #inv_lines_dict = dict(val for val in inv_lines.iteritems())    
            #columns = ('id')
	    #results = map(lambda x: (dict(zip(columns, x))), [inv_id])
	    res = dtx([{'id': inv_id}], custom_root='ID', attr_type=False)
	    cur.close()
	    return Response(res, content_type='application/XML; charset=utf-8')
        except Exception as e:
	    cur.close()
            return str(e)

#importIMOSInvoice:
    def importTCOInvoice1(self, transaction_no, invoice_date):
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


