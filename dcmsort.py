###################################################################################################
# 
#    Sorting a DICOM folder
# 
# -The purpose of this script is to go through all folders and encrypt all patient names within said
# folders.
# 
# -To run, Anaconda Python must be installed (https://www.continuum.io/downloads)
# -After installation, open up the Anaconda Command Prompt (http://i.imgur.com/xeWBABj.png)
# -Type in "pip install pydicom" (http://i.imgur.com/NgZvgJM.png)
# -Move the dcmanon.py file to the current directory (C:\User\USERNAME\Anaconda)
# -To test out, type in: 
# python dcmsort.py -d "ZYOURFULLDIRECTORYHERE" 
# -d = directory
# 
# Created by: Adrian Lam (ayplam@gmail.com)
# Date: 2015-12-28
# 
###################################################################################################

import os
import dicom
import time
import shutil
from dicom.filereader import InvalidDicomError
from optparse import OptionParser
from multiprocessing import Pool
import pandas as pd
import re

isdcmname = re.compile('IMAGE\.\d{4}\.\d{4}')

def readDicomFile(filename):

    try:
        dcminf = dicom.read_file(filename)
    except IOError:
        return []
    except InvalidDicomError:
        return []
    except:
        return []

    out = []
    if dcminf:

        fields = ['SeriesInstanceUID','ProtocolName','SeriesNumber','InstanceNumber']
        out.append(filename)

        for field in fields:
            if hasattr(dcminf, field):
                out.append(getattr(dcminf, field))
            else:
                out.append('')

    return out

def getDicomAttr(dcm):
    dcminfo = dcm[0]
    attr = dcm[1]

    if hasattr(dcminfo, attr):
        return getattr(dcminfo, attr)
    else:
        return ''

def renameDicom(dcm):
    oldname = dcm[0]
    newname = dcm[1]

    try:
        os.rename(oldname,newname)
    except:
        0

def moveDicom(dcm):
    src = dcm[0]
    dst = dcm[1]
    try:
        shutil.move(src, dst)
    except:
        return

# Determines whether a folder has already been sorted based on the naming scheme
def isFolderSorted(filenames):

    tmp = [1 if isdcmname.findall(dcm) else 0 for dcm in filenames]
    return all(tmp)

def test(df):
    return str(df['sn']) + '@@' + str(df['in'])

def main():
    start_time = time.time()

    parser = OptionParser(usage="usage: %prog [options] filename")
    parser.add_option("-d", "--dir",
                      action="store", # optional because action defaults to "store"
                      type="string",
                      dest="directory",
                      default='',
                      help="Directory file to search through")

    (options, args) = parser.parse_args()

    base = options.directory

    if not base:
        print "No directory selected! Please follow pattern to choose directory: "
        print "python dcmsort.py -d C:\YOURDIRECTORYHERE"

    p = Pool(10)

    fldrs_failed_rename = []

    for dirname, dirnames, filenames in os.walk(base):

        # Check if folder has been previously sorted
        if isFolderSorted(filenames):
            continue

        fullfilenames = [os.path.join(dirname, filename)for filename in filenames]

        dcminfo_container = p.map(readDicomFile, fullfilenames)
        dcminfo_container = filter(None, dcminfo_container)

        # If dcminfo_container is empty, continue
        if not dcminfo_container:
            continue

        print "Sorting ", dirname

        # df_dcminfo = pd.DataFrame({'sid': series_uid, 'pn': protocol_name, 'sn': series_number, 'in': image_number})
        df_dcminfo = pd.DataFrame(dcminfo_container, columns =['fn', 'sid', 'pn', 'sn', 'in'])

        df_dcminfo.sort(['sn', 'in'])

        df_dcminfo['sn2'] = df_dcminfo['sn'].apply(lambda x: '000' + str(x))
        df_dcminfo['in2'] = df_dcminfo['in'].apply(lambda x: '000' + str(x))

        # new dicom filenames
        df_dcminfo['fname'] = 'IMAGE.' + df_dcminfo['sn2'].apply(lambda x: x[-4:]) + '.' + \
            df_dcminfo['in2'].apply(lambda x: x[-4:])

        # Find out if you're dealing with many different dicoms or just a single folder
        uniq_sid = df_dcminfo['sid'].unique()
        uniq_pn = df_dcminfo['pn'].unique()

        fldrs_to_rename = []


        if len(uniq_sid) == 1 | len(uniq_pn) == 1:
            # Rename all filenames
            df_dcminfo['fn2'] = dirname + '\\' + df_dcminfo['fname']
            tuple_rename = zip(df_dcminfo.fn, df_dcminfo.fn2)
            p.map(renameDicom, tuple_rename)

            # Rename the directory so it follows "DCMXXXX_PROTOCOLNAME"
            new_fldr = 'DCM' + df_dcminfo['sn2'][0][-4:] + \
                '_' + df_dcminfo['pn'][0]
            fullfldrname = os.path.join(os.path.split(dirname)[0], new_fldr).upper()

            fldrs_to_rename.append((dirname,fullfldrname))

        else:
            # Need to create new folders for everything
            df_dcminfo['fldr'] = 'DCM' + df_dcminfo['sn2'].apply(lambda x: x[-4:]) + \
                '_' + df_dcminfo['pn']

            fullfldrname = [os.path.join(dirname, fldr) for fldr in df_dcminfo['fldr'].unique().tolist()]
            fldrs_to_make = []

            for fldr in fullfldrname:
                if not os.path.isdir(fldr):
                    fldrs_to_make.append(fldr)

            p.map(os.mkdir, fldrs_to_make)

            # Create new filenames
            df_dcminfo['fn2'] = dirname + '\\' + df_dcminfo['fldr'] + '\\' + df_dcminfo['fname']
            tuple_move = zip(df_dcminfo.fn, df_dcminfo.fn2)

            p.map(moveDicom,tuple_move)

        # Sleep for a little bit so it has time to "complete moving" before renaming folders
        time.sleep(0.25)

        # Leave all folder renaming for the end because it interferes at time with file renaming.
        for fldr_pair in fldrs_to_rename:
            if not os.path.isdir(fldr_pair[1]):
                for x in xrange(5):
                    try:
                        # Use the pool to parallelze encryption
                        os.rename(fldr_pair[0],fldr_pair[1])
                    except:
                        # If it's the fourth try, print the failed directory.
                        fldrs_failed_rename.append(fldr_pair)
                        continue

                    break

                try:
                    os.rename(fldr_pair[0],fldr_pair[1])
                except:
                    fldrs_failed_rename.append(fldr_pair)

    # There is a second try in case the first try fails. Not the best way to do it, but deal with it.
    for fldr_pair in fldrs_failed_rename:
        for x in xrange(5):
            try:
                os.rename(fldr_pair[0],fldr_pair[1])
            except:
                
                if x == 4:
                    print "DIRECTORY FAILED TO ANONYMIZE: ", fldr_pair
                # Continue onto next "iteration" if it fails; skip the break
                continue

            break

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
