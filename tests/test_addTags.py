import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import journalTools as jtls

csvfile = "/SNS/SNAP/IPTS-33219/shared/journal_IPTS33219.csv"
runs = []


jtls.add_tag_to_runs(csvfile, runs = [65891,65892,65893,65894,65895,65896],
             tag= "bruciteA")

jtls.add_tag_to_runs(csvfile, runs = [65898,65899,65900,65901,65902,65904],
             tag= "bruciteB")

jtls.add_tag_to_runs(csvfile, runs = [65905,65906,65907,65908],
             tag= "bruciteC")
