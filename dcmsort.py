import os
import dicom
import time
import shutil
from dicom.filereader import InvalidDicomError
from optparse import OptionParser
from multiprocessing import Pool
import pandas as pd



def readDicomFile(filename):

    try:
        dcminf = dicom.read_file(filename)
    except IOError:
        return []
    except InvalidDicomError:
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
    os.rename(oldname,newname)

def moveDicom(dcm):
    src = dcm[0]
    dst = dcm[1]
    shutil.move(src, dst)

def test(df):
    return str(df['sn']) + '@@' + str(df['in'])

def main():
    start_time = time.time()

    base = 'E:\Users\Adrian Lam\Dropbox\ForOshinski\Random\PythonScripts\SamplePatient2'

    # base ='C:\Users\mri\Dropbox\ForOshinski\Random\PythonScripts'
    p = Pool(3)

    for dirname, dirnames, filenames in os.walk(base):

        print dirname
        fullfilenames = [os.path.join(dirname, filename)for filename in filenames]

        dcminfo_container = p.map(readDicomFile, fullfilenames)
        dcminfo_container = filter(None, dcminfo_container)

        # df_dcminfo = pd.DataFrame({'sid': series_uid, 'pn': protocol_name, 'sn': series_number, 'in': image_number})
        df_dcminfo = pd.DataFrame(dcminfo_container, columns =['fn', 'sid', 'pn', 'sn', 'in'])
        df_dcminfo.sort_values(['sn', 'in'])

        df_dcminfo['sn2'] = df_dcminfo['sn'].apply(lambda x: '000' + str(x))
        df_dcminfo['in2'] = df_dcminfo['in'].apply(lambda x: '000' + str(x))

        # new dicom filename
        df_dcminfo['fname'] = 'IMAGE.' + df_dcminfo['sn2'].apply(lambda x: x[-4:]) + '.' + \
            df_dcminfo['in2'].apply(lambda x: x[-4:])

        uniq_sid = df_dcminfo['sid'].unique()
        uniq_pn = df_dcminfo['pn'].unique()

        if len(uniq_sid) == 1 | len(uniq_pn) == 1:
            # Rename all filenames
            df_dcminfo['fn2'] = dirname + '\\' + df_dcminfo['fname']
            tuple_rename = zip(df_dcminfo.fn, df_dcminfo.fn2)
            p.map(renameDicom, tuple_rename)

            # Rename the directory
        else:
            # Need to create new folders for everything
            df_dcminfo['fldr'] = 'DCM' + df_dcminfo['sn2'].apply(lambda x: x[-4:]) + \
                '_' + df_dcminfo['pn']

            fullfldrname = [os.path.join(dirname, fldr) for fldr in df_dcminfo['fldr'].unique().tolist()]
            p.map(os.mkdir, fullfldrname)

            # Create new filenames
            df_dcminfo['fn2'] = dirname + '\\' + df_dcminfo['fldr'] + '\\' + df_dcminfo['fname']
            tuple_move = zip(df_dcminfo.fn, df_dcminfo.fn2)

            p.map(moveDicom,tuple_move)

    # Put this into a pandas database. It'll be faster and easier to deal with.

    # If multiple seriesUID and the protocol name length > 1
    # if len(set(seriesUID)) > 1 & len(set(protocolName)) > 1:
    # 	# This likely means you have a hundreds of images that need to be sorted into their respective folders
    #
    # else if len(set(seriesUID)) == 1:

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
