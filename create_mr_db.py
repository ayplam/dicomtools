###################################################################################################
# 
#    Creating an Dicom Database
# 
# -The purpose of this script is to create an excel file that contains all the dicom information
# that you would want. To add fields to the database, add the dicom fields to "fields" and 
# "dcmfields". 
# 
# -To run, Anaconda Python must be installed (https://www.continuum.io/downloads)
# -After installation, open up the Anaconda Command Prompt (http://i.imgur.com/xeWBABj.png)
# -Type in "pip install pydicom" (http://i.imgur.com/NgZvgJM.png)
# -Move the create_mr_db.py file to the current directory (C:\User\USERNAME\Anaconda)
# -To test out, type in: 
# python create_mr_db.py -d "Z:\Images\Databases\SamplePatient" -f "MRDB_SamplePatient.csv" -o "Z:\Images\Databases"
# -d = directory
# -f = filename
# -o = location of output
# (http://i.imgur.com/DQ5qpo0.png)
# 
# To run the script multiple patient folders: 
# 1. create a .bat file (Example : Z:\Images\Databases\SamplePatient\create_database.bat)
# 2. Move the .bat file to (C:\User\USERNAME\Anaconda)
# 3. Type in: create_database.bat (http://i.imgur.com/HLTZTRq.png?1) OR double click the .bat file
# 
# Created by: Adrian Lam (ayplam@gmail.com)
# Date: 2015-11-30
# 
###################################################################################################

import glob
import os
import pandas as pd
import dicom
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool
import sys
from dicom.filereader import InvalidDicomError
from optparse import OptionParser

# fields should correspond to dcmfields (except the last two "nimgs" and "dir")
# ie, pxname = PatientName; pxid = PatientID. The locations make a difference for where the columns will be located
fields = ['pxname','pxid','datestr','acqtstr','studyuid','seriesuid','mrseq','manu','fa','tr','te','ti']
dcmfields = ['PatientName','PatientID','StudyDate','AcquisitionTime','StudyInstanceUID','SeriesInstanceUID', \
             'SeriesDescription', 'Manufacturer','FlipAngle','RepetitionTime','EchoTime','InversionTime']

# These are the indices to check whether you have the same series or not.
inds = [0,1,2,4,5,6]
			 
def get_db_dicominfo(filename):
    
    cnt = 0;

    dcm_inf_holder = []

    dcminf = []
    dcmvals = []

    # v0: Loop through all files until you find a dicom image, or until you reach 
    # the end of all files

    # v1: Loop throughu ALL files. Find dicom files with unique PatientName/PatientID/UIDs (as notated by inds)
    # This is MUCH slower, but it finds multiple series within a folder.
    cnt += 1;

    # Error catching in case the file is not a valid dicom.
    try:
        dcminf = dicom.read_file(filename)
    except:
        return []

    dcmvals = []
    # If dicominfo exists, append all selected "dcmfields" into the list.
    if dcminf:
        for cnt,dcmfield in enumerate(dcmfields):
            if hasattr(dcminf,dcmfield):
                dcmvals.append( getattr(dcminf,dcmfield) )
            else:
                dcmvals.append(None)

    else:
        # If no dicom image, set all dicom values to None (will appear as blank).
        for dcmfield in dcmfields:
            dcmvals.append(None)
            
        dcmvals[2] = '19000101'
        dcmvals[3] = '000000'
        
    
    # At the end, append the number of files in the folder (presumably number of dicoms)
    # as well as the directory
    # dcmvals.append(len(all_files))
    # dcmvals.append(os.getcwd())

    # if bIsDcmValUnique & (dcmvals[3] != '000000'):
    #     dcm_inf_holder.append(dcmvals)

    # Return a dictionary
    return dict(zip(fields,dcmvals))

def remove_same_series(dcm_inf_holder):

    # Since there can be different series within a folder, this ensures that all series are captured. 
    return [dict(y) for y in set(tuple(x.items()) for x in dcm_inf_holder)]



# Main loops through all folders
def main():

    # base = 'Z:/Images/NeoStem AMR/AMR-002_132019 - Sorted/'
    fields = ['pxname','pxid','datestr','acqtstr','studyuid','seriesuid','mrseq','manu','fa','tr','te','ti','nimgs','dir']
    export = ['pxname','pxid','date','acqt','studyuid','seriesuid','mrseq','manu','fa','tr','te','ti','nimgs','dir']

    # Example (need to change directory to where create_mr_db.py is located):
    # create_mr_db.py -f neostem_database.csv -d "Z:\Users\Shared\Sleepystuff\images\EFFERVESCENT PATIENTS"
    parser = OptionParser(usage="usage: %prog [options] filename")
    parser.add_option("-f", "--filename",
                      action="store",
                      type="string",
                      dest="filename",
                      default='MR_Database.csv',
                      help="Name of output csvfile")
    parser.add_option("-d", "--dir",
                      action="store", # optional because action defaults to "store"
                      type="string",
                      dest="directory",
                      default=os.getcwd(),
                      help="Directory file to search through")
    parser.add_option("-o", "--output",
                      dest="output",
                      default=os.getcwd(),
                      help="Choose whether to store folder with no dicoms")    
    parser.add_option("-n", "--null",
                      action="store_false",
                      dest="nullfolder",
                      default=False,
                      help="Choose whether to store folder with no dicoms")

    (options, args) = parser.parse_args()
    
    # base is the base directory to search in and get ALL subfolders
    base = options.directory

    # csvname is the output filename
    csvname = options.filename

    p = Pool(10)

    cnt = 0

    # os.walk goes through every subdirectory as a loop. The current directory
    # in the loop is "dirname". All files in dirname are in a list, "filenames"
    for dirname, dirnames, filenames in os.walk(base):

        df = pd.DataFrame(data = None, columns = fields)        

        fullfilenames = [os.path.join(dirname,filename) for filename in filenames]
        test = p.map(get_db_dicominfo,fullfilenames)
        test = filter(None,test)
        print dirname
        print len(test)
        test = remove_same_series(test)
        print "SAME SERIES REMOVED: ", len(test)

        # # Should have an option to append all empty folders without dicoms to the dataframe or not.
        # df = pd.DataFrame(data = info, columns = fields)
        # cnt+=1

        # # Failsafe in case the date or acquisition time doesn't exist. For some reason, this is actually a thing.
        # df['date'] = df['datestr'].apply(lambda s: '1900-00-00' if not s else datetime(year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8])))
        # df['acqt'] = df['acqtstr'].apply(lambda s: '00:00:00' if not s else str(s[:2]+':'+s[2:4]+':'+s[4:6]))

        # # Outputs are always appended to the csvfile. Nothing is ever overwritten.
        # with open(options.output + '/' + csvname, 'a') as f:
        #     df[export].to_csv(f,sep=',',header=False)
            

if __name__ == '__main__':
    main()