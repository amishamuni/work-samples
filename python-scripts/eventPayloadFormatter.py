# Event Payload Formatter
#
# Given a text file with the JSON string for Event Payload, decipher it in a human readable way
# 
# Relies on having csv files generated for a flat view on ID and event name
# Future improvement could be to generate them via this script itself if they are not found
#
# Input : give your eventPayload in a text file
#         you need to remove the preceeding text ( put an example )
#
# Output : Human readable event payload
#
# Reference : 1A SDK Generic Event 
#             JOIN gives us list <GenericEvent>
#             1 item : Main Domain Events
#             All other items : Correlation Set Events, which we dont use
#
# Samples :
# =========
# ****Registry.json item from interns Mongo collection. Of interest is the protoId and the path
#  {
#      "_id": "d72e98ba-23f6-4739-b558-e79eb1bdef6b",
#      "deprecated": false,
#      "functionalName": "Pnr Header",
#      "name": "pnrHeader",
#      "nullable": true,
#      "path": "PassengerNameRecord.pnrHeader",
#      "protoId": "7",
#      "repeated": false,
#      "type": "com.amadeus.pulse.message.pnr.PnrHeader"
#  }
# *****Generic event structure from my notes, we get a LIST of Generic Events
#  {
#    id, version, schema_version,              ONE per domain (e.g. Main PNR / Correlation Set1 / Correlation Set2 )
#	 events
#	 [ 
#	    {                                      ONE Root level event, and everything is nested WITHIN in
#		    eventId, 
#		    data,                              Among other things, the root one will give the name of the proto , e.g. "com.amadeus.pulse.message.PassengerNameRecord"
#		    eventType, 
#		    events                             
#		    [ 
#      		     {},{},...                     The eternal structure continues
#		    ] 
#		}
#	 ]
#  }
#
# *****ChangedData section comments from my notes
# Sample map_key tattoo : PNQQ4O-2019-11-19-OT-13
# Root -> protoID 37 for PDL, map key gives tattoo -> 9 for Acceptance detail -> 5 for channel type
#                                                                             -> 4 for channel originator
#                                                  -> 6 for Status detail
#                                                  -> 2 for Source ID
# *****Issues noted 
#
#
#
#
#


#imports
import json
import time
import csv

#Print Event type better as its an Enum
def printEventType(eventType):
    eventTypeString = ""

    if eventType == 0:
       eventTypeString = "NONE"
    elif eventType == 1:
       eventTypeString = "CREATED"
    elif eventType == 2:
       eventTypeString = "UPDATED"
    elif eventType == 3:
       eventTypeString = "DELETED"

    return eventTypeString	

#Check the proto full name at top level to know which registy file to check
#I dont have all registries right now
def getRegistryFileNameForDomain(topLevelProtoFullName):
    registryFileName = ""
    if topLevelProtoFullName == "com.amadeus.pulse.message.PassengerNameRecord":
        registryFileName = "registry/pnrRegistry.csv"
    elif topLevelProtoFullName == "com.amadeus.pulse.message.FlightDate":
        registryFileName = "registry/skdRegistry.csv"		
    elif topLevelProtoFullName == "com.amadeus.pulse.message.TravelDocument":
        registryFileName = "registry/tktRegistry.csv"
    return registryFileName


#Super inefficient, needs definite revamp
def printEventName(eventID, registryFileName):
    eventName = ""
    with open(registryFileName) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[0] == eventID:
                eventName = row[2]
                #Allow ourselves to identify the Singular events as we face duplicates
                if( row[3] != "" ):
                    eventName += (':' + str(row[3]))
                break

    csvfile.close()

    return eventName

#Format the changed data
#Refer its structure here : https://rndwww.nce.amadeus.net/git/projects/AGP/repos/pulse-java-sdk/browse/streaming/sdk-message/src/main/protobuf/sdk/genericEvent.proto#75
#I aim to print field name, old value and new value
#Doesnt cater right now to the "path" being complex - i.e. repeated_index or map_key
def printChangedData(changedData, rootRegistryItem, dataRegistryReader):
    fieldPath = ""
    oldValue = ""
    newValue = ""
    fieldType = ""
    repeatedIndex = ""
    mapKey = ""	

    #For Created events, we wont have a old payload reference section at all	
    if "new_payload_reference" in changedData:
        if "path" in changedData["new_payload_reference"]:
            id = changedData["new_payload_reference"]["path"][0]["id"]
            #Look for the protoID in the passed rootRegistryItem
            fields = rootRegistryItem["fields"]

            for i in fields:
                if (int(i['protoId']) == id):
                    fieldPath = i['path']
                    fieldType = i['type']
                    
                    #Some items can have a complex path, which can take a repeated index or a map key, in addition to the ID
                    if "repeated_index" in changedData["new_payload_reference"]["path"][0]:
                        repeatedIndex = changedData["new_payload_reference"]["path"][0]["repeated_index"]
                        fieldPath += ( ':'	+ str(repeatedIndex) )

                    if "map_key" in changedData["new_payload_reference"]["path"][0]:
                        mapKey = changedData["new_payload_reference"]["path"][0]["map_key"]
                        fieldPath += ( ':' + str(mapKey) )
						
                    break
                    
    #For Deleted events, we wont have a new payload reference section at all
    if "old_payload_reference" in changedData:
        if "path" in changedData["old_payload_reference"]:
            id = changedData["old_payload_reference"]["path"][0]["id"]
            #Look for the protoID in the passed rootRegistryItem
            fields = rootRegistryItem["fields"]

            for i in fields:
                if (int(i['protoId']) == id):
                    fieldPath = i['path']
                    fieldType = i['type']
                    
                    #Some items can have a complex path, which can take a repeated index or a map key, in addition to the ID
                    if "repeated_index" in changedData["old_payload_reference"]["path"][0]:
                        repeatedIndex = changedData["old_payload_reference"]["path"][0]["repeated_index"]
                        fieldPath += ( ':'	+ str(repeatedIndex) )

                    if "map_key" in changedData["old_payload_reference"]["path"][0]:
                        mapKey = changedData["old_payload_reference"]["path"][0]["map_key"]
                        fieldPath += ( ':' + str(mapKey) )
						
                    break
					
    #If we have the old and new values for the field, lets print that too
    #The field value is a OneOf between "string_value", "int_value", "boolean_value"
    if "new_field_value" in changedData:
        newValue = (list((changedData["new_field_value"]).values())[0])

    if "old_field_value" in changedData:
        oldValue = str((list((changedData["old_field_value"]).values())[0]))

    return fieldPath,fieldType,oldValue,newValue


#Print top level data
def printTopLevelEventData(genericEvent, myOutput):

    #As the top level is still an array though everything is nested within the first item
	#This is the ROOT level event for the events within this genericEvent item
    topLevelEvent = genericEvent["events"][0]
    oldVersion = ""
	
    if "old_payload_reference" in topLevelEvent["data"]:
        oldVersion = topLevelEvent["data"]["old_payload_reference"]["version"]

    functionalID = topLevelEvent["data"]["new_payload_reference"]["functional_identifier"]
    newVersion = topLevelEvent["data"]["new_payload_reference"]["version"]

    #Fixed for protoFullName instead of "6"
    protoFullName = topLevelEvent["data"]["new_payload_reference"]["protoFullName"]
    eventRegistryFileName = ""
    eventRegistryFileName = getRegistryFileNameForDomain(protoFullName)
	
    if eventRegistryFileName == "":
       return

    topLevelEventID = topLevelEvent["event_id"]
	#Fix for event type instead of "4"
    topLevelEventType = topLevelEvent["event_type"]

    myOutput.write('\n***********************************\n')
    myOutput.write('Functional ID:%s,Old Version:%s,New Version:%s,Proto Full Name:%s\n' % (functionalID, oldVersion, newVersion, protoFullName))
    print('Functional ID:%s,Old Version:%s,New Version:%s,Proto Full Name:%s\n' % (functionalID, oldVersion, newVersion, protoFullName))
	
    #FORCED to do this as passing the genericEvent["events"] messes the rootRegistry for resulting loop  
    myOutput.write('EventID,EventName,FieldPath,OldValue,NewValue\n')
    myOutput.write('%s,%s\n' % ( topLevelEventID, printEventType(topLevelEventType)))
    myOutput.write('**Nested under ROOT\n')
    print('%s,%s\n' % ( topLevelEventID, printEventType(topLevelEventType)))
	
    #Supply the data registry without map so that we can get the field names from event payload protobuf IDs
    dataRegistry = open("dataRegistryWithoutMaps.json", 'r')
    dataRegistryReader = json.load(dataRegistry)

    #Data Registry has nothing for the Correlation items
    rootRegistryItem = ""	
    for i in dataRegistryReader:
        if i["importClass"] == protoFullName:
            rootRegistryItem = i
            break

#   We now need to go through every nested Event and each of them can have nested Events within them
#   Check if nested Events exist - e.g. sometimes you have only root event in Create
#   Note had Issue passing genericEvent["events"] as the rootRegistry is then incorrect - same root is needed for genericEvent["events"] and topLevelEvent["events"]
    if "events" in topLevelEvent:
        print('Looping nested Events\n')	
        printNestedEventData( topLevelEvent["events"], myOutput, eventRegistryFileName, rootRegistryItem, dataRegistryReader, False)
    
    dataRegistry.close() 

# Nested Event handling
# We aim to print the ID, the name and the changed data corresponding to the event payload we received
# First element we enter with is the full list of Events within the ROOT event
# I introduce an ugly "viaNested" just to print an event nbr within the ROOT event, so a cat display of my csv is more readable..to me :)
def printNestedEventData(eventContainer, myOutput, eventRegistryFileName, dataRegistryItem, dataRegistryReader, viaNested):

    #This is actually looping through every event
    #Each item could in turn have many nested Event or NONE nested
    if viaNested == False:
        eventNbr = 1

    for j in range(len(eventContainer)):
        eventID =  eventContainer[j]["event_id"]
        if viaNested == False:
            myOutput.write('++Event number: %s\n' % (eventNbr))
            print('++Event number: %s\n' % (eventNbr))			

        #No Event registry for correlation yet
        eventName = ""
        if( eventRegistryFileName != ""):
            eventName = printEventName(eventID, eventRegistryFileName)

        #Changed data section : try to get the Field, FieldPath, Old Value & New Value
        fieldPath = ""
        fieldType = ""
        oldValue = ""
        newValue = ""
        if "data" in eventContainer[j]:
            changedData = eventContainer[j]["data"]
            fieldPath,fieldType,oldValue,newValue = printChangedData(changedData, dataRegistryItem, dataRegistryReader)

        #Write what we found
        myOutput.write('%s,%s,%s,%s,%s\n' % (eventID,eventName,fieldPath,oldValue,newValue))

        #if there are nested events i need to call my function again, passing the right dataRegistryItem onward
        if "events" in eventContainer[j] and fieldType != "":
            for i in dataRegistryReader:
                if i["importClass"] == fieldType:
                    nestedDataRegistryItem = i
                    myOutput.write('++++Nested within this event: %s %s\n' % (eventID,eventName))							
                    printNestedEventData(eventContainer[j]["events"], myOutput, eventRegistryFileName, nestedDataRegistryItem, dataRegistryReader, True)
	
        if viaNested == False:
            eventNbr += 1
		
# Main processing
# Consider taking some input parameters atleast for the input file

# Open files 
timestr = time.strftime("%Y%m%d-%H%M%S")
myOutput = open('analysedEvent/analysedEvent' + timestr + '.csv',"w")
print("Generating file in analysedEvent")
# Build Flat Event Registry files
# Not called here, run it beforehand to keep things simple

# Start decoding the payload
mypayload = open("ACCP/TSZ9KR-2020-05-14-v0.txt", 'r')
data = json.load(mypayload)

for i in data:
#   As we consume from JOIN we get a list of GenericEvent, one for the main domain the rest for Correlation
    printTopLevelEventData( i, myOutput)

# Close files
myOutput.close()
mypayload.close()
print("Generation complete: analysedEvent/analysedEvent%s.csv" % timestr)
