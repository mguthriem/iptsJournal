This is intended to be downloaded and used on the NScD analysis.sns.gov cluster. Given a proposal IPTS number (e.g. 12345), it will generate a `.csv` file in the corresponding IPTS-12345, containing useful information for each run in the IPTS. 

**Note: Currently it's set up to work for the SNAP beamline, but could be generalised.**

Basic usage: 

1. Generate a `.csv` journal file for ipts-12345

```
python iptsJournal 12345
```

2. Add two optional pv logs as columns in the `.csv`

```
python iptsJournal 12345 BL3:SE:Teledyne1:Pressure BL3:SE:Teledyne2:Pressure
```   
