from flask import Flask
from flask import request
from Helpers.postgresconnector import *
from xml.dom.minidom import parse
import xml.dom.minidom
from lxml import etree
import xmltodict
from dicttoxml import dicttoxml

#initialize Flask
app = Flask(__name__)

#clientId
#1.android clientId
validClientIds = ['ERP5C31F0FAA628D40CD2920A79D8841597B']

#@app.route('/echo', methods = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE'])
@app.route('/', methods = ['GET','POST','PUT'])
def ping():
    return 'Welcome'


@app.route('/checkdb', methods = ['GET'])
def checkdb():
    try:
	return PostgresConnector().ConnectToDatabase()
    except Exception as e:
	return str(e)

@app.route('/testXML', methods = ['POST'])
def testXML():
    rawfile = request.data
    print"======rawfile=======", rawfile
    dom  = xmltodict.parse(request.data)['invoice']#['xml']['From']
    for key, value in dom.iteritems():
	print"======key, value======", key, value
    #dom = etree.fromstring(rawfile)
    print"=======dom=====",dom
    #DOMTree = xml.dom.minidom.parse(data)
    #print"=======DOMTree========", DOMTree
    #collection = DOMTree.documentElement
    #print"======collection==========", collection
    #if collection.hasAttribute("shelf"):
     #  print "Root element : %s" % collection.getAttribute("shelf")
    array = {"name": "Mohit", "Age": 25}
    xml = dicttoxml(array, custom_root='test', attr_type=False)
    print"=======xml=====", xml
    return Response(xml, content_type='application/XML; charset=utf-8')

#APIs using

@app.route('/v1/CheckUser', methods = ['POST'])
def UserCheck():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    loginKey = data['loginKey']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    if (loginKey and loginKey.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400
    try:
	return PostgresConnector().GetUserId(loginKey)
    except Exception as e:
	print str(e)


@app.route('/v1/getCurrencyRate', methods = ['GET', 'POST'])
def getCurrencyRate():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    currency = json_data.get('currency',False) and json_data.get('currency',False)[0] or ''
    date = json_data.get('date',False) and json_data.get('date',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400	
    return PostgresConnector().getCurrencyRate(currency, date)

@app.route('/v1/importIMOSInvoice', methods = ['POST'])
def importIMOSInvoice():
    rawfile = request.data
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400	
    transNo = ''
    invoiceDate = ''
    try:
       if rawfile:  
    	  dom  = xmltodict.parse(rawfile)['invoice'] #['xml']['From']
          for key, value in dom.iteritems(): 
	      #print"======key, value======",  key, value    #value['@xmlns:xsi']
	      if key == 'transNo':
		 transNo = value
	      if key == 'invoiceDate':
	         invoiceDate = value
	  #print"=====transNo====", transNo, invoiceDate
	  if transNo and invoiceDate:
             return PostgresConnector().importIMOSInvoice(transNo, invoiceDate)
	  return "Some Error Occured"
    except:
	return "Some Error Occured"
    #dom = etree.fromstring(rawfile)
    #print"=======dom=====",dom
    #DOMTree = xml.dom.minidom.parse(data)
    #print"=======DOMTree========", DOMTree
    #collection = DOMTree.documentElement
    #print"======collection==========", collection
    #if collection.hasAttribute("shelf"):
     #  print "Root element : %s" % collection.getAttribute("shelf")
    #array = {"name": "Test", "Age": 12}
    #xml = dicttoxml(array, custom_root='test', attr_type=False)
    #print"=======xml=====", xml
    #return Response(xml, content_type='application/XML; charset=utf-8')

@app.route('/v1/importFreightInvoice', methods = ['POST'])
def importFreightInvoice():
    rawfile = request.data
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400	
    transNo = ''
    invoiceDate = ''
    try:
       if rawfile:  
    	  dom  = xmltodict.parse(rawfile)['invoice'] #['xml']['From']
          for key, value in dom.iteritems(): 
	      #print"======key, value======",  key, value    #value['@xmlns:xsi']
	      if key == 'transNo':
		 transNo = value
	      if key == 'invoiceDate':
	         invoiceDate = value
	  #print"=====transNo====", transNo, invoiceDate
	  if transNo and invoiceDate:
             return PostgresConnector().importFreightInvoice(transNo, invoiceDate)
	  return "Some Error Occured"
    except:
	return "Some Error Occured"



@app.route('/v1/TestImport', methods = ['POST'])
def TestImport():
    rawfile = request.data
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400	
    transNo = ''
    invoiceDate = ''
    try:
       if rawfile:  
    	  dom  = xmltodict.parse(rawfile)
          for key, value in dom.iteritems(): 
	      print("======key, value===={}=={}==".format(key, value))    #value['@xmlns:xsi']
	      
	  return "Success"
    except:
	return "Some Error Occured"


#run Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000,debug=False,threaded=True)
