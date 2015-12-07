###################################################################################################
# 
#    Anonymizing a DICOM folder
# 
# -The purpose of this script is to go through all folders and encrypt all patient names within said
# folders.
# 
# -To run, Anaconda Python must be installed (https://www.continuum.io/downloads)
# -After installation, open up the Anaconda Command Prompt (http://i.imgur.com/xeWBABj.png)
# -Type in "pip install pydicom" (http://i.imgur.com/NgZvgJM.png)
# -Move the dcmanon.py file to the current directory (C:\User\USERNAME\Anaconda)
# -To test out, type in: 
# python dcmanon.py -d "Z:\Images\Databases\SamplePatient" 
# -d = directory
# 
# Created by: Adrian Lam (ayplam@gmail.com)
# Date: 2015-11-30
# 
###################################################################################################

import re
import os
import dicom
import numpy as np
import sys
from dicom.filereader import InvalidDicomError
from optparse import OptionParser
from multiprocessing import Pool

def create_ascii_encrypt_key():
    pi = '314159265358979323846264338327\
950288419716939937510582097494\
459230781640628620899862803482\
534211706798214808651328230664\
709384460955058223172535940812\
848111745028410270193852110555\
964462294895493038196442881097\
566593344612847564823378678316\
527120190914564856692346034861\
045432664821339360726024914127'
    
    seq = np.arange(32,128)
    randomizer = np.empty(0)

    while (seq.size != 0) & (len(pi) > 1):
        if int(pi[:2]) < seq.size:
            randomizer = np.append(randomizer,seq[int(pi[:2])])
            seq = np.delete(seq,int(pi[:2]))
            pi = pi[2:]
        elif int(pi[:1]) < seq.size:
            randomizer = np.append(randomizer,seq[int(pi[:1])])
            seq = np.delete(seq,int(pi[:1]))
            pi = pi[1:]
        else:
            # Skip number
            pi = pi[1:]

    return randomizer

# Global variables
KEY = create_ascii_encrypt_key();
fields_to_anon = ['PatientsName','MedicalAlerts','PatientsAddress','SpecialNeeds']

def encrypt_dicom_name(dcm):

    # Check if the file is a dicom
    dcmname = dcm[0]
    opts = dcm[1]
    bool_encrypt = opts[0]
    bool_digitcheck = opts[1]

    bool_write = False

    dcminf = []

    try:
        dcminf = dicom.read_file(dcmname)
    except IOError:
        0
    except InvalidDicomError:
        0

    # If it is a dicom, scramble all information.
    if dcminf:

        for field in fields_to_anon:

            # Check to make sure dicom field exists.
            if hasattr(dcminf,field):
                name = getattr(dcminf,field)
				
                # If it's not a string, skip
                if not isinstance(name, basestring):
                    continue

                if bool_encrypt:
                    # encrypt the dicomfield

                    if bool_digitcheck:
                        bool_hasdigits = re.findall('\d+',name)
                    else:
                        bool_hasdigits = False

                    # Ignore if the name has the words "anonymous" or "volunteer" in it
                    if ( name.lower().find("anonymous") >= 0 ) | ( name.lower().find("volunteer") >= 0 ):
                        continue

                    # The additional "_JNO" ending is a safety to prevent items 
                    # from being re-encrypted. It is assumed that if the name 
                    # has any numbers, the patient field has already been anonymized.
                    if (name[-4:] != "_JNO") & (not bool_hasdigits):

                        anon_name = encrypt_string(name,KEY) + "_JNO"
                        setattr(dcminf,field,anon_name)
                        bool_write = True

                else:

                    # If the name has "_JNO" as the ending, it has been encrypted
                    # and needs to be unencrypted.
                    if name[-4:] == "_JNO":
                        anon_name = unencrypt_string(name[:-4],KEY)
                        setattr(dcminf,field,anon_name)
                        bool_write = True

        if bool_write:
            dicom.write_file(dcmname,dcminf)


def encrypt_string(string,randomizer):
    encryptd_string = ''
    for char in string:
        encryptd_string += chr(int(randomizer[ord(char)-32]))
        
    return encryptd_string

def unencrypt_string(string,randomizer):
    unencrypt = np.argsort(KEY) + 32
    unencryptd_string = ''
    for char in string:
        unencryptd_string += chr(int(unencrypt[ord(char)-32]))
        
    return unencryptd_string
    

def main():

	# Example (need to change directory to where create_mr_db.py is located):
    # create_mr_db.py -f neostem_database.csv -d Z:\Users\Shared\Sleepystuff\images\EFFERVESCENT PATIENTS
    parser = OptionParser(usage="usage: %prog [options] filename")
    parser.add_option("-d", "--dir",
                      action="store", # optional because action defaults to "store"
                      type="string",
                      dest="directory",
                      default=os.getcwd(),
                      help="Directory file to search through")
    parser.add_option("-f", "--filename",
                      action="store", # optional because action defaults to "store"
                      type="string",
                      dest="filename",
                      default="",
                      help="Directory file to search through")    
    parser.add_option("-v", "--verbose",
                      action="store_true", # optional because action defaults to "store"
                      dest="verbose", # Do you want the script to show ALL directories it's going through?
                      help="Show current folder")
    parser.add_option("-u", "--anon",
                      action="store_false" , # optional because action defaults to "store"
                      dest="anon",          # flag to encrypt names
                      default=True,         # default option is to encrypt
                      help="Flag to decrypt dicom fields. Default is to encrypt fields")
    parser.add_option("-n", "--numbers",
                      action="store_true" , # optional because action defaults to "store"
                      dest="numbers",          # flag to encrypt names
                      default=False,
                      help="Check for numbers (0-9) in the field. If numbers exist in field, do not encrypt")

    (options, args) = parser.parse_args()
    
    
    # Allow a textfile to be read to automatically anonymize multiple folders
    if options.filename:
        with open(options.filename,'r') as f:
            dirs = f.read()

        dirs = dirs.split('\n')
        directories_to_anonymize = filter(None, dirs)
    else:
    # If no text file is specified, anonymize current directory
        directories_to_anonymize = options.directory


    # Create a tuple to send over to the encrypt_dicom_name
    opt_tuple = (options.anon,options.numbers)

    p = Pool(10) # Use 10 processes to paralleze dicom anonymization

    # base is the base directory to search in and get ALL subfolders
    for base in directories_to_anonymize:

        print "Current main directory:", base

        if options.verbose:
            if options.anon:
                print "Encrypting the following folders..."
            else:
                print "Decrypting the following folders..."

        # os.walk goes through every subdirectory as a loop. The current directory
        # in the loop is "dirname". All files in dirname are in a list, "filenames"
        for dirname, dirnames, filenames in os.walk(base):
            if options.verbose:
                print dirname

            #Append dicom options to each item        
            fullfilenames_options = [ [os.path.join(dirname,filename),opt_tuple] for filename in filenames]

            # Use the pool to parallelze encryption
            p.map(encrypt_dicom_name,fullfilenames_options)


if __name__ == '__main__':
    main()