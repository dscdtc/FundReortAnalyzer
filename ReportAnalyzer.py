import os
from tqdm import tqdm
from pandas import pandas as pd
import pdfplumber


def pdfphrase(fpath:str, fname:str, key='期末持有的基金份额', isTable=True):
    with pdfplumber.open(fpath + fname) as pdf:
        for page in pdf.pages[50:]:
            texts = page.extract_text()
            if key in texts:
                if isTable:
                    data = page.extract_table()
                    data = pd.DataFrame(data[1:], columns=data[0]).dropna(how='all')
                    # data.insert(0, 'fname', fname)
                    return data
                else:
                    return texts
        return pd.DataFrame([fname], columns=['fname'])

def main():
    fpath = '.\\PDFfiles\\'
    allf = os.listdir(fpath)
    res = pd.DataFrame()

    for fname in tqdm(allf[:2]):
        # re = pdfphrase(fpath, fname, '期末持有的基金份额')
        re = pdfphrase(fpath, fname, '期末基金管理人的从业人员持有本开放式基金份额总量区间情况')
        re.to_excel('.\\%s.xlsx'%fname, index=False)
        res.append(re)
        print(res)

    res.to_excel('.\\result.xlsx', index=False)

if __name__ == "__main__":
    main()
