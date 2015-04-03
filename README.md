# milestones-db
db files for amp.pharm.mssm.edu/milestones/

Files needed for amp.pharm.mssm.edu/milestones/

All .xlsx files except for milestones consolidated are raw files given from centers.
milestones-consolidated.xlsx is the excel file created by combining all six raw files.

To recreate the db, run db-DSGC-consolidated.py from the same directory as milestones-consolidated.txt

Running hmsParser.py will rebuild the Harvard phase one database through their APIs.

After running hmsParser.py, you may run hmsParserCons.py to consolidate the KINOMEscan and KiNativ assay entries in MongoDB into two respective MongoDB documents with unique samples.
