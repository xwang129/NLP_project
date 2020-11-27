"""
this file is for tf.idf term weighting scheme for two different dictionaries, i.e. we compute each wij, where i
represents ith word in the selected dictionary, and j represents jth document in our collection. Since we only
care about dow30 stocks, so, take collection as all 10-K files that correspond to our 39 companies
"""
import pandas as pd
import os
import re
import numpy as np
import Load_MasterDictionary as LM
from find_Dow30 import *


def clean_dow30_file(dow30_file_path='dow30_complete.xlsx', ticker_file_path='ticker.txt'):
    # use cik to identify files that belong to the company
    dow30 = pd.read_excel(dow30_file_path)
    all_tickers = pd.read_csv(ticker_file_path, delimiter='\t', header=None)

    dow30['thru'] = dow30['thru'].replace('.', '20201126')
    dow30['co_tic'] = dow30['co_tic'].apply(lambda x: str.lower(x))
    all_tickers = all_tickers.rename(columns={0: 'co_tic', 1: 'cik'})

    dow30 = dow30.merge(all_tickers, how='left', on='co_tic')

    dow30.iloc[:, 1:].to_excel('../Data/stock_and_index/dow30_complete_cleaned.xlsx')

    return dow30


def get_all_relevant_files(root_path='G:/NLP-P1-Textdata/',
                           perYear_path='../Data/stock_and_index/dow30_PerYear_cik.xlsx',
                           result_path='G:/NLP-P1-Textdata/filtered_textdata/'):
    # if the file is the one we needed, read it and output it to result path
    years = [str(year) for year in range(2010, 2019)]
    qtrs = ['QTR' + str(i) for i in range(1, 5)]

    df_Dow30 = pd.read_excel(perYear_path, index_col=0)

    for year in years:
        for qtr in qtrs:
            filtered_file_list = []
            file_list = os.listdir(root_path+year+'/'+qtr)
            year_dow30 = list(df_Dow30.iloc[:, years.index(year)])

            for file in file_list:
                start = re.search("data_", file).span()
                end = file.find("_", start[1])
                cik = int(file[start[1]:end])

                if cik in year_dow30 and '10-K' in file:
                    in_file = root_path+year+'/'+qtr+'/'+file
                    with open(in_file, 'r', encoding='UTF-8', errors='ignore') as f_in:
                        doc = f_in.read()
                    if not os.path.exists(result_path+year+'/'+qtr): os.makedirs(result_path+year+'/'+qtr)
                    out_file = result_path+year+'/'+qtr+'/'+file
                    with open(out_file, 'w', encoding='UTF-8', errors='ignore') as f_out:
                        f_out.write(doc)


def generate_tfidf_weight(lm_path="../Data/dictionary/LoughranMcDonald_MasterDictionary_2018.csv",
                          hv_path="../Data/dictionary/HIV-4.csv",
                          relevant_data_path="G:/NLP-P1-Textdata/filtered_textdata/",
                          result_path="../Data/"):
    """
    use this function to generate tf.idf weighting scheme,
    assume our collection is all 10-K files of dow30 stocks

    compute tf.idf for two dictionaries, lm(fin-neg) and harvard
    save two dataframes as pkl file
    :return: None
    """
    # get all relevant text data file path
    relevant_files = []
    years = [str(year) for year in range(2010, 2019)]
    qtrs = ['QTR' + str(i) for i in range(1, 5)]

    for year in years:
        for qtr in qtrs:
            root = relevant_data_path+year+'/'+qtr
            if os.path.exists(root):
                relevant_files.extend([root+'/'+x for x in os.listdir(relevant_data_path+year+'/'+qtr)])

    # relevant_files = relevant_files[0:3]

    lm_dict = LM.load_masterdictionary(lm_path, True)
    hv_dict = pd.read_csv(hv_path)

    hv_dict['Entry'] = hv_dict['Entry'].apply(lambda s: ''.join(x for x in s if x.isalpha()))
    hv_dictionary = hv_dict.drop_duplicates(subset=['Entry'], keep='first')
    hv = hv_dictionary.loc[hv_dictionary['Negativ'] == 'Negativ', ['Entry', 'Negativ']]
    negativefields = list(hv['Entry'].values)

    lm_words = list(lm_dict.keys())
    hv_words = list(hv_dict['Entry'].values)

    N = len(relevant_files)

    lm_tfidf = pd.DataFrame(index=range(N), columns=lm_words)
    hv_tfidf = pd.DataFrame(index=range(N), columns=hv_words)

    file_doc_list = []

    for j in range(len(relevant_files)):
        try:
            file = relevant_files[j]
            with open(file, 'r', encoding='UTF-8', errors='ignore') as f_in:
                doc = f_in.read()
            doc_len = len(doc)
            doc = re.sub('(May|MAY)', ' ', doc)  # drop all May month references
            doc = doc.upper()  # for this parse caps aren't informative so shift

            file_doc_list.append(doc)

            tf_ij_lm = np.zeros(len(lm_dict))
            tf_ij_hv = np.zeros(len(hv_dict))

            tokens = re.findall('\w+', doc)
            num_of_words_lm = 0
            num_of_words_hv = 0

            for token in tokens:
                if not token.isdigit() and len(token) > 1 and token in lm_words and lm_dict[token].negative:
                    tf_ij_lm[lm_words.index(token)] += 1
                    num_of_words_lm += 1
                if not token.isdigit() and len(token) > 1 and token in negativefields:
                    tf_ij_hv[hv_words.index(token)] += 1
                    num_of_words_hv += 1

            tf_ij_lm = 1 + np.log(1+tf_ij_lm)
            tf_ij_hv = 1 + np.log(1+tf_ij_hv)
            tf_ij_lm[np.isinf(tf_ij_lm)] = 0
            tf_ij_hv[np.isinf(tf_ij_hv)] = 0

            lm_tfidf.iloc[j, :] = tf_ij_lm / (1 + np.log(num_of_words_lm))
            hv_tfidf.iloc[j, :] = tf_ij_hv / (1 + np.log(num_of_words_hv))

            print("end of file "+str(j))
        except:
            print("ERROR "+str(j))
            continue

    try:
        idf_lm = list(np.log(N/(lm_tfidf.values>0).sum(axis=0)))
        idf_hv = list(np.log(N/(hv_tfidf.values>0).sum(axis=0)))

        lm_tfidf *= idf_lm
        hv_tfidf *= idf_hv

    except:
        print("LAST ERROR, NOT DIVIDED")

    lm_tfidf.to_csv(result_path+"lm_tfidf.csv")
    hv_tfidf.to_csv(result_path+"hv_tfidf.csv")


if __name__ == '__main__':
    # clean_dow30_file()
    # get_all_relevant_files()
    generate_tfidf_weight()

    # tf.idf summary file
    lm_tfidf = pd.read_csv("../Data/lm_tfidf.csv")
    hv_tfidf = pd.read_csv("../Data/hv_tfidf.csv")

    lm = lm_tfidf.sum(axis=0)
    hv = hv_tfidf.sum(axis=0)
    pd.DataFrame({"LM": lm, "HV": hv}).to_csv("../Data/Neg_LMHV.csv")