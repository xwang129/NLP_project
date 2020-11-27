"""
Program to provide generic parsing for all files in user-specified directory.
The program assumes the input files have been scrubbed,
  i.e., HTML, ASCII-encoded binary, and any other embedded document structures that are not
  intended to be analyzed have been deleted from the file.

Dependencies:
    Python:  Load_MasterDictionary.py
    Data:    LoughranMcDonald_MasterDictionary_XXXX.csv

The program outputs:
   1.  File name
   2.  File size (in bytes)
   3.  Number of words (based on LM_MasterDictionary)
   4.  Proportion of positive words (use with care - see LM, JAR 2016)
   5.  Proportion of negative words
   6.  Proportion of uncertainty words
   7.  Proportion of litigious words
   8.  Proportion of modal-weak words
   9.  Proportion of modal-moderate words
  10.  Proportion of modal-strong words
  11.  Proportion of constraining words (see Bodnaruk, Loughran and McDonald, JFQA 2015)
  12.  Number of alphanumeric characters (a-z, A-Z)
  13.  Number of digits (0-9)
  14.  Number of numbers (collections of digits)
  15.  Average number of syllables
  16.  Average word length
  17.  Vocabulary (see Loughran-McDonald, JF, 2015)

  ND-SRAF
  McDonald 2016/06 : updated 2018/03
"""

import csv
import glob
import re
import string
import sys
import time
import os
# sys.path.append('D:\GD\Python\TextualAnalysis\Modules')  # Modify to identify path for custom modules
# import Load_MasterDictionary as LM

import dask
import functools
from dask import compute, delayed

import pandas as pd

# CORE_NUM = int(os.environ['NUMBER_OF_PROCESSORS'])
CORE_NUM = 1


def parLapply(CORE_NUM, iterable, func, *args, **kwargs):
    with dask.config.set(scheduler='processes', num_workers=CORE_NUM):
        f_par = functools.partial(func, *args, **kwargs)
        result = compute([delayed(f_par)(item) for item in iterable])[0]
        return result

# User defined directory for files to be parsed
# TARGET_FILES = r'G:/NLP-P1-Textdata/*.*'

# User defined file pointer to LM dictionary
# MASTER_DICTIONARY_FILE = r'G:\NLP-P1-Textdata\dictionary\LoughranMcDonald_MasterDictionary_2018.csv'
DICTIONARY_FILE = r'../Data/dictionary/HIV-4.csv'

# User defined output file
# OUTPUT_FILE = r'G:/NLP-P1-Textdata/Temp/Text-File-Statistics.csv'

# Setup output, add company name, CONFORMED PERIOD OF REPORT, FILED AS OF DATE, DATE AS OF CHANGE
OUTPUT_FIELDS = ['file name', 'file size', 'company name', 'conformed period of report', 'filed as of date',
                 'date as of change', 'number of words', '% positive', '% negative',
                 '% uncertainty', '% litigious', '% modal-weak', '% modal moderate',
                 '% modal strong', '% constraining', '# of alphabetic', '# of digits',
                 '# of numbers', 'avg # of syllables per word', 'average word length', 'vocabulary']

# load master dictionary, do not return other variables
# lm_dictionary = LM.load_masterdictionary(MASTER_DICTIONARY_FILE, True)
dictionary = pd.read_csv(DICTIONARY_FILE, index_col = 0).T


# main function
def count_sentiment(file_list, OUTPUT_FILE):
    """
    write each statistics for a file as a row in OUTPUT_FILE
    :param file_list: a list of text files to be summarized
    :return: None
    """
    f_out = open(OUTPUT_FILE, 'w')  # create a file in write mode
    # see https://www.runoob.com/python/python-func-open.html
    wr = csv.writer(f_out, lineterminator='\n')
    wr.writerow(OUTPUT_FIELDS)  # head row, note that each row is represented by a list

    # file_list = glob.glob(TARGET_FILES)  # see https://blog.csdn.net/huhuandk/article/details/86317026
    # text files waiting to parse

    def write_stat(file):
        with open(file, 'r', encoding='UTF-8', errors='ignore') as f_in:
            doc = f_in.read()  # load all content of file specified above, doc: <class 'str'>
        doc_len = len(doc)
        doc = re.sub('(May|MAY)', ' ', doc)  # drop all May month references
        doc = doc.upper()  # for this parse caps aren't informative so shift

        output_data = get_data(doc)

        output_data[0] = file
        output_data[1] = doc_len
        wr.writerow(output_data)

    for file in file_list: write_stat(file)


def get_data(doc):

    vdictionary = {}
    _odata = [0] * 21  # same length as OUTPUT_FIELDS
    # total_syllables = 0
    word_length = 0

    # company name
    try:
        start = re.search("COMPANY CONFORMED NAME:", doc, flags=0).span()
        end = re.search("CENTRAL INDEX KEY:", doc, flags=0).span()
        _odata[2] = doc[start[1] + 1:end[0]].strip()
    except:
        _odata[2] = None

    # conformed period of report
    try:
        start = re.search("CONFORMED PERIOD OF REPORT:", doc, flags=0).span()
        end = re.search("FILED AS OF DATE:", doc, flags=0).span()
        _odata[3] = doc[start[1] + 1:end[0]].strip()
    except:
        _odata[3] = None

    # filed as of date
    start = re.search("FILED AS OF DATE:", doc, flags=0).span()
    try:
        end = re.search("DATE AS OF CHANGE:", doc, flags=0).span()
    except:
        end = re.search("FILER:", doc, flags=0).span()
    _odata[4] = doc[start[1] + 1:end[0]].strip()

    # date as of change
    try:
        start = re.search("DATE AS OF CHANGE:", doc, flags=0).span()
        end = re.search("FILER:", doc, flags=0).span()
        _odata[5] = doc[start[1] + 1:end[0]].strip()
    except:
        _odata[5] = None

    tokens = re.findall('\w+', doc)  # Note that \w+ splits hyphenated words

    for token in tokens:
        if not token.isdigit() and len(token) > 1 and token in dictionary:
            _odata[6] += 1  # word count, the seventh
            word_length += len(token)
            if token not in vdictionary:
                vdictionary[token] = 1
            # 3-10 of _odata records sentiment count
            # print(dictionary)
            # print(dictionary[token])
            if dictionary[token].Positiv == 'Positiv': _odata[7] += 1
            if dictionary[token].Negativ == 'Negativ': _odata[8] += 1
            # if dictionary[token].uncertainty: _odata[9] += 1
            # if dictionary[token].litigious: _odata[10] += 1
            # if dictionary[token].weak_modal: _odata[11] += 1
            # if dictionary[token].moderate_modal: _odata[12] += 1
            # if dictionary[token].strong_modal: _odata[13] += 1
            # if dictionary[token].constraining: _odata[14] += 1
            # total_syllables += dictionary[token].syllables

    _odata[15] = len(re.findall('[A-Z]', doc))  # '# of alphabetic,'
    _odata[16] = len(re.findall('[0-9]', doc))  # '# of digits,'
    # drop punctuation within numbers for number count
    doc = re.sub('(?!=[0-9])(\.|,)(?=[0-9])', '', doc)
    doc = doc.translate(str.maketrans(string.punctuation, " " * len(string.punctuation)))
    _odata[17] = len(re.findall(r'\b[-+\(]?[$€£]?[-+(]?\d+\)?\b', doc))  # '# of numbers,'
    # _odata[18] = total_syllables / _odata[6]  # 'avg # of syllables per word,'
    _odata[19] = word_length / _odata[6]  # 'average word length,'
    _odata[20] = len(vdictionary)  # 'vocabulary'
    
    # Convert counts to %
    for i in range(7, 14 + 1):
        _odata[i] = (_odata[i] / _odata[6]) * 100
    # Vocabulary
        
    return _odata


if __name__ == '__main__':
    # we generate summary data for all text file downloaded, i.e. 2006-2018
    # print('\n' + time.strftime('%c') + '\nGeneric_Parser.py\n')
    # # construct file list
    years = [str(year) for year in range(2010, 2019)]
    quaters = ['QTR' + str(i) for i in range(1, 5)]
    # root_path = 'G:/NLP-P1-Textdata/'
    root_path = '../Data/filtered_textdata/'
    #
    for year in years:
        for quater in quaters:
            file_list = os.listdir(root_path+year+'/'+quater)
            file_list = [root_path+year+'/'+quater+'/'+file for file in file_list]
            OUTPUT_FILE = '../Data/Output-Stat_h/Text-File-Statistics-'+year+'-'+quater+'.csv'
            count_sentiment(file_list, OUTPUT_FILE=OUTPUT_FILE)
            print("END OF QUATER "+quater)
        print("END OF YEAR "+year)

    # year = str(2019)
    # quater = 'QTR4'
    # file_list = os.listdir(root_path + year + '/' + quater)
    # file_list = [root_path+year+'/'+quater+'/'+file for file in file_list]
    # # OUTPUT_FILE = 'G:/NLP-P1-Textdata/Output-Stat/Text-File-Statistics-'+year+'-'+quater+'.csv'
    # OUTPUT_FILE = '../Data/Output-Stat_h/Text-File-Statistics-'+year+'-'+quater+'.csv'
    # count_sentiment(file_list, OUTPUT_FILE=OUTPUT_FILE)

    # print('\n' + time.strftime('%c') + '\nNormal termination.')
