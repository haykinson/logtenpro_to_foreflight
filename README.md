Convert LogTeni Pro logbooks to ForeFlight Logbook import files
====

Please note, this is for LogTen Pro, and not LogTen Pro X. The latter has official instructions from ForeFlight on their logbook website.

Instructions
----

1. Install Python 2.7, from https://www.python.org/
2. Download convert.py from above, and put it in some directory.
3. On your iPad / iPhone, go into Settings and make sure you have at least one Mail account set up. 
4. In LogTen Pro, go to the Help section of LogTen Pro, ensure that the Attach Logbook toggle is enabled, and then tap Email Support. This will pop up a form to send an email to Coradine support. Change the destination to your own email address, and hit Send. This should send you a copy of the logbook database, in a zip file. You can see how to do this in more detail in Coradine's own video -- https://youtu.be/0uK78_vOmR4?t=1m17s
5. When you get the email, download the zip file, unzip it, and find the database file. It should be called something like LogTenCoreDataStore.sql -- put it in the same directory as convert.py
6. Open a command prompt and run `python convert.py LogTenCodeDataStore.sql > foreflight.csv` -- this should produce a new file, foreflight.csv, that's formatted according to Foreflight's guidelines.
7. Go to plan.foreflight.com/logbook and try to import the file we just generated.

Some caveats:

- This was built with my logbook in mind, and so it has some pecularities (like not tracking all the fields, since I didn't care about some of them or never used them).
- It's pretty limited in making data look clean and takes some liberties with aircraft type names and models.
- If you make improvements, please feel free to fork and submit a pull request.

