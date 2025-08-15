import csv
import ast
import os
import h5py   
import numpy as np


def parseAndValidateRunString(runInfo):

    # Parse the run string and validate runs

    run_str =  runInfo["runString"]
    inst = runInfo["inst"]
    ipts = runInfo["ipts"]

    runs = set()
    for part in run_str.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            runs.update(range(start, end + 1))
        else:
            runs.add(int(part))

    availableRuns = []
    for run in runs: 
        #check that run data exist and can be accessed
        if os.path.exists(f"/SNS/{inst}/IPTS-{ipts}/nexus/{inst}_{run}.nxs.h5"):
            availableRuns.append(run)   

    percentAvailable = len(availableRuns) / len(runs) * 100
    print(f"Total of {len(runs)} runs found. Can access data from {percentAvailable}% of these")

    return sorted(availableRuns)

def cleanKey(fullKey):

    # strips detail from full key used to locate data in nxs file: 

    removeList = ["entry/","DASlogs/","/average_value","/value"]

    for item in removeList:
        fullKey = fullKey.replace(item,"")

    return fullKey

def dataFromNXS(run,runInfo,keys):

    # given a run number, runInfo dictionary and log name, return a dictionary corresponding to the values of 
    # the logs in the list dataItems 

    NXSFilePath = f"/SNS/{runInfo['inst']}/IPTS-{runInfo['ipts']}/nexus/{runInfo['inst']}_{run}.nxs.h5"
    nxs = h5py.File(NXSFilePath, 'r')

    fSize = os.path.getsize(NXSFilePath)

    dict = {"run":run,
            "filesize(Gb)": fSize / (1024 ** 3),  # Convert bytes to GB
            }
    for key in keys:
        h5object = None
        if type(key) == list:
            #this is a list of alternative keys use the first one that exists
            for altKey in key:
                try:
                    h5object = nxs[altKey][:]
                    key = altKey
                    break
                except:
                    continue
        else: 
            h5object = nxs[key][:]

        if h5object is None:
            print(f"Key {key} not found in run {run}.")
            continue

        if h5object.ndim > 1:
            for i in range(h5object.ndim):
                # print(h5object[i])
                val = h5object[0]

        else:
            dataObject = h5object[0]

            # need some processing here
            if isinstance(dataObject, (bytes, np.bytes_)):
                val = dataObject.decode("utf-8")
            else:
                val = dataObject

        
        dict[cleanKey(key)]=val

        # print(f"key: {key} has type {type(dataObject)} value: {val}")
    # print(dict)
    return dict


def genSNAPState(floatVars,IntVars):

  import hashlib
  import json

  det_arc1 = floatVars[0]
  det_arc2 = floatVars[1]
  wav = floatVars[2]
  freq = floatVars[3]
  GuideIn = IntVars[0]

  from dataclasses import dataclass

  # https://docs.python.org/3/library/dataclasses.html
  @dataclass
  class StateId:
      vdet_arc1: float
      vdet_arc2: float
      WavelengthUserReq: float
      Frequency: int
      Pos: int

      # Round inputs to reduced number of possible states
      def __init__(self, vdet_arc1: float, vdet_arc2: float, WavelengthUserReq: float, Frequency: float, Pos: int):
          self.vdet_arc1 = float(round(vdet_arc1 * 2) / 2)
          self.vdet_arc2 = float(round(vdet_arc2 * 2) / 2)
          self.WavelengthUserReq = float(round(WavelengthUserReq, 1))
          self.Frequency = int(round(Frequency))
          self.Pos = int(Pos)


  stateID = StateId(vdet_arc1=det_arc1, vdet_arc2=det_arc2, WavelengthUserReq=wav, Frequency =freq, Pos=GuideIn)
  hasher = hashlib.shake_256()

  decodedKey = json.dumps(stateID.__dict__).encode('utf-8')

  hasher.update(decodedKey)

  hashedKey = hasher.digest(8).hex()

  return hashedKey

def createJournal(runInfo, optionalLogs=[]):
    # Create the journal directory if it doesn't exist
    journalPath = f"/SNS/{runInfo['inst']}/IPTS-{runInfo['ipts']}/shared/journal_IPTS{runInfo['ipts']}.csv"
    runList = runInfo["runs"]

    new_rows = []
    #each row is represented by a dictionary
    for run in runList:

        metaKeys = [
            "entry/duration",
            "entry/title",
            "entry/start_time",
            "entry/end_time",
            'entry/DASlogs/det_arc1/value',
            'entry/DASlogs/det_arc2/value',
            ['entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value',
            'entry/DASlogs/BL3:Chop:Gbl:WavelengthReq/value'], # use list to get either of these
            'entry/DASlogs/BL3:Det:TH:BL:Frequency/value',
            'entry/DASlogs/BL3:Mot:OpticsPos:Pos/value',
        ]
        metaKeys.extend(optionalLogs)

        #retrieve metadata from nxs file for this run
        meta = dataFromNXS(run,runInfo,metaKeys)

        #calculate stateID
        if "BL3:Chop:Skf1:WavelengthUserReq" in meta.keys():
            wav = meta["BL3:Chop:Skf1:WavelengthUserReq"]
        elif "BL3:Chop:Gbl:WavelengthReq" in meta.keys():
            wav = meta["BL3:Chop:Gbl:WavelengthReq"]
        else:
            wav = 0.0
            print("No wavelength found in run metadata can\'t calculate stateID")

        meta["stateID"] = genSNAPState([meta["det_arc1"],
                                        meta["det_arc2"],
                                        wav,
                                        meta["BL3:Det:TH:BL:Frequency"],
                                        0.0],
                                        [meta["BL3:Mot:OpticsPos:Pos"],0])
        
        # process some specific logs to make a bit more readable
        meta["filesize(Gb)"] = round(meta["filesize(Gb)"],3)
        meta["duration(min)"] = round(meta["duration"]/60,1)
        del meta["duration"]
        meta["start_time"] = meta["start_time"][0:19]
        meta["end_time"] = meta["end_time"][0:19]
        meta["tags"] = []

        new_rows.append(meta)

    # Specify desired column order
    desired_order = [
        "run",
        "title",
        "duration(min)",  # your new key, placed before start_time
        "start_time",
        "end_time",
        "filesize(Gb)",
        "tags"
        # add more known keys here if needed
    ]
    # Add any additional keys that may be present but not listed
    if new_rows:
        for key in new_rows[0].keys():
            if key not in desired_order:
                desired_order.append(key)
    else:
        desired_order = []

    if not os.path.exists(journalPath):
        # Create new CSV with default columns
        with open(journalPath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=desired_order)
            writer.writeheader()
            for row in new_rows:
                writer.writerow(row)
        print(f"Journal created with {len(new_rows)} runs.")
        return

    # Read existing CSV
    with open(journalPath, "r", newline="") as f:
        reader = csv.DictReader(f)
        existing_rows = list(reader)
        existing_columns = reader.fieldnames
        if existing_columns is None:
            existing_columns = desired_order.copy()

    # # Add any new optionalLogs columns to existing columns
    # for col in optionalLogs:
    #     if col not in existing_columns:
    #         existing_columns.append(col)

    # Find existing run numbers
    existing_run_numbers = set(int(row["run"]) for row in existing_rows if "run" in row and row["run"].isdigit())

    # Only append new runs to the CSV
    new_run_numbers = set(runList) - existing_run_numbers
    new_run_rows = [row for row in new_rows if int(row["run"]) in new_run_numbers]

    with open(journalPath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=existing_columns)
        writer.writeheader()
        for row in existing_rows:
            # Ensure all columns exist in each row
            for col in existing_columns:
                if col not in row:
                    row[col] = ""
            writer.writerow(row)
        for row in new_run_rows:
            writer.writerow(row)

    print(f"Journal updated. {len(new_run_rows)} new runs added.")


def add_tag_to_runs(csv_file, runs, tag):
    """
    Append a tag to the 'tags' list for all rows whose 'run' value
    is in the given list of runs.

    Parameters:
        csv_file (str): Path to the CSV file to update.
        runs (list[int]): List of run numbers to update.
        tag (str): Tag to append to each matching row's 'tags' list.
    """
    runs_set = set(runs)  # faster lookups
    rows = []
    
    # Read all rows and update tags where needed
    with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            run_val = int(row['run'])
            tags_list = ast.literal_eval(row['tags']) if row['tags'] else []
            if run_val in runs_set:
                if tag not in tags_list:
                    tags_list.append(tag)
            row['tags'] = str(tags_list)
            rows.append(row)

    # Write updated rows back to the CSV
    with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
