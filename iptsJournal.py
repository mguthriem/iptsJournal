from finddata import cli
import sys, os
import journalTools as jtls 

inst = sys.argv[1].upper()
ipts = sys.argv[2]

try: 
    runString = cli.getRunsInProp("SNS",inst,f"IPTS-{ipts}")
except Exception:
    print(f"Could not find any runs for {inst} in IPTS-{ipts}.")
    print("Please check the instrument name and IPTS number and, if these are correct, that you have permission to access this IPTS.")
    sys.exit(1)

runInfo = {
     "ipts": ipts,
     "inst": inst,
     "runString": runString
}

runList = jtls.parseAndValidateRunString(runInfo)

if not runList:
        print("No valid runs found. Exiting.")
        sys.exit(0)

runInfo["runs"] = runList

optionalLogs = []
if len(sys.argv) >= 4:
    #additional logs have been specified
    optionalLogs = []
    for i in range(3, len(sys.argv)):
        optionalLogs.append(f"entry/DASlogs/{sys.argv[i]}/average_value")

print(f"attempting to create journal for IPTS-{ipts} on {inst} including optional logs: {optionalLogs}")
jtls.createJournal(runInfo, optionalLogs)


