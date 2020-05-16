# Event Registry Creator
# Amisha Muni, DEC 2019
#
# Given text files of Event detection Config, generate flat csvs with the ID, Path and human readable name
# Given ETL teams have generated config with duplicate Event IDs, also indicate which are generated with "singular" option in the config to avoid issues in Rule 
# For more details on this "singular" issue please refer https://rndwww.nce.amadeus.net/agile/browse/PULS-16763
# Generated Flat files should be stored manually at https://rndwww.nce.amadeus.net/confluence/display/AGPL/RE+Flat+files
# 
# Pre-req:
# Store a PNREventDetectionConfig.txt, SKDEventDetectionConfig.txt, TKTEventDetectionConfig.txt looking at the raw files in the respective repos from ETL
#
# Output:
# Generates a pnrRegistry.csv, skdRegistry.csv, tktRegistry.csv
# You need to manually save them as xlsx after formatting it better
#
# TODO:
# Get files from repo directly in future and use the JSON file rather than a txt file of event detection configs
# Append a timestamp to generated CSV files, today this will overwrite any existing file
# Maybe generate the xlsx itself rather than the current csv
# Better filenames for the generated registries as we have many versions like PNR v1, PNR v2 etc
# Correlation event files too
# Input parameters to ask which repos people want to generate a flat view of and if they want to supply the event detection config or let the script get it from the repo
#

#Imports
import json

#Cater to nested Events for EventRegistry
def buildEventRegistryItem(eventType, eventRegistry, rootName):
#   Root level
    root = False

    if "pathToProtocolBufferField" not in eventType:
        root = True
		
    for j in eventType["events"]:
        eventID = j["id"]
        eventPath = ""
        eventDescription = ""
        singular = ""		
        if rootName != "":
            eventPath = rootName
            eventDescription = rootName			

        if root == False:
            eventPath += eventType["pathToProtocolBufferField"]
            eventDescription = eventType["pathToProtocolBufferField"]

        eventDescription += "_" + j["eventSubType"]  

        #PNR atleast has events duplicated - once as singular, once as nested
        #Domain data is nested and so its the nested Event which is used in Rule		
        if "treatAsSingularField" in eventType:
            singular = "SINGULAR"		
			
        eventRegistry.write('%s,%s,%s,%s\n' % (eventID, eventPath, eventDescription, singular))
		

#   If nested eventTypes
    if "eventTypes" in eventType:
        extendedRootName = ""
        if rootName != "":
            extendedRootName = rootName

        if root == False:
            extendedRootName += eventType["pathToProtocolBufferField"] + "/"
        else:
            extendedRootName += "/"

        for k in eventType["eventTypes"]:
            buildEventRegistryItem(k, eventRegistry, extendedRootName)
        
#Build an Event Registry
def buildEventRegistry(eventRegistry, eventConfigData, rootDomain):

    eventRegistry.write('Protocol Buffer Schema Version: %s\n' % (eventConfigData["protocolBuffersSchemaVersion"]))
    eventRegistry.write('EventID, GeneratedEventPath, GeneratedEventDescription, Singular\n')
    allEventTypes = eventConfigData["eventTypes"]
    for i in allEventTypes:
        buildEventRegistryItem(i, eventRegistry, rootDomain)

#Main
#Note it doesnt delete older files, will overwrite
print("\n Generating registry/pnrRegistry.csv")
pnrRegistry = open("registry/pnrRegistry.csv",'w')
pnrEventDetectionConfig = open("reference/PNREventDetectionConfig.txt", 'r')
pnrEventConfigData = json.load(pnrEventDetectionConfig)
buildEventRegistry(pnrRegistry, pnrEventConfigData, "PNR")
pnrEventDetectionConfig.close()
pnrRegistry.close()

print("\n Generating registry/skdRegistry.csv")
skdRegistry = open("registry/skdRegistry.csv",'w')
skdEventDetectionConfig = open("reference/SKDEventDetectionConfig.txt", 'r')
skdEventConfigData = json.load(skdEventDetectionConfig)
buildEventRegistry(skdRegistry, skdEventConfigData, "SKD")
skdEventDetectionConfig.close()
skdRegistry.close()

print("\n Generating registry/tktRegistry.csv")
tktRegistry = open("registry/tktRegistry.csv",'w')
tktEventDetectionConfig = open("reference/TKTEventDetectionConfig.txt", 'r')
tktEventConfigData = json.load(tktEventDetectionConfig)
buildEventRegistry(tktRegistry, tktEventConfigData, "TKT")
tktEventDetectionConfig.close()
tktRegistry.close()

print("\n All done!")