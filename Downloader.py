# /usr/bin python
# encoding: utf-8
import os
import time
import json
from pandas import pandas as pd
import requests
from tqdm import tqdm
from fake_useragent import UserAgent

KINDS = {
    '年报': 'category_ndbg_jjgg',
    '半年报': 'category_bndbg_jjgg',
    '季报': 'category_jdbg_jjgg',
    '所有': 'category_ndbg_jjgg;category_bndbg_jjgg;category_jdbg_jjgg'
}

def getpdfurl(codes, sdate, edate, kind='年报', filter=False):
    if os.path.exists('.\\temp.xlsx'):
        allpdf = pd.read_excel('.\\temp.xlsx')
        return allpdf
    sdate = pd.Timestamp(sdate).strftime('%Y-%m-%d')
    edate = pd.Timestamp(edate).strftime('%Y-%m-%d')

    if codes =='':
        stocks = ''
    else:
        exit("下载指定基金的代码暂未实现，目前仅支持批量下载")
        # TODO: 基金代码映射
        # ords = a[codes]
        # stocks = codes + ',' + ords


    params = {
        'pageNum': '1',
        'pageSize': '30',
        'column': 'fund',
        'tabName': 'fulltext',
        'plate':'' ,
        'stock': stocks,
        'searchkey':'' ,
        'secid':'' ,
        'category': KINDS[kind],
        'trade':'' ,
        'seDate': '{}~{}'.format(sdate,edate),
        'sortName': '',
        'sortType': '',
        'isHLtitle': 'true'
    }
    
    url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    headers = {"User-Agent": UserAgent(verify_ssl=False).random}
    response_comment = requests.post(url,params = params,headers = headers )
    res = json.loads(response_comment.text)
    
    n = len(res['announcements'])
    totpages = res['totalpages']
    
    allpdf = []
    print("Start to gather pdf url:")
    for i in tqdm(range(1, totpages + 1)):
        params = {
        'pageNum': str(i),
        'pageSize': '30',
        'column': 'fund',
        'tabName': 'fulltext',
        'plate':'' ,
        'stock': stocks,
        'searchkey':'' ,
        'secid':'' ,
        'category': 'category_ndbg_jjgg',
        'trade':'' ,
        'seDate': '{}~{}'.format(sdate,edate),
        'sortName': '',
        'sortType': '',
        'isHLtitle': 'true'}
        
        url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        response_comment = requests.post(url,params = params )
        res = json.loads(response_comment.text)
        for k in range(n):
            allpdf.append(pd.DataFrame.from_dict(res['announcements'][k],orient='index').T)

    allpdf = pd.concat(allpdf,axis = 0).reset_index(drop = True)
    allpdf = allpdf[['secName','secCode','announcementTitle','adjunctUrl']]
    allpdf = allpdf.drop_duplicates(
        # subset=['announcementTitle','adjunctUrl'], # 去重列，按这些列进行去重
        subset=['announcementTitle'], # 重名基金仅保留第一个数据
        keep='first' # 保存第一条重复数据
    )

    # 保留股票型和混合型
    if filter:
        allpdf['ifstock'] = allpdf.announcementTitle.map(
            lambda x:'股票型' in x or '混合型' in x and '公告' not in x
        )
        allpdf = allpdf.loc[allpdf.ifstock].reset_index(drop = True)

    allpdf.to_excel('.\\temp.xlsx', index=False)
    print("To download pdf count is %d !"%len(allpdf))
    return allpdf

def getFundReportpdf(allpdf,fpath):

    headers = {"User-Agent": UserAgent(verify_ssl=False).random}
    print("Start to download pdf:")
    for k in tqdm(range(allpdf.shape[0])):
        fname = '{}.pdf'.format(allpdf.announcementTitle[k])
        if os.path.exists(fpath + fname):
            continue

        url = allpdf.adjunctUrl[k]
        urls = 'http://static.cninfo.com.cn/{}#navpanes=0&toolbar=0&statusbar=0&pagemode=thumbs&page=1'.format(url)
        r = requests.get(urls, timeout = 300,headers = headers)

        with open (fpath + fname,'wb') as f:
            f.write(r.content)
        f.close()
        time.sleep(2)

if __name__ == "__main__":
    fpath = '.\\PDFfiles\\'
    allpdf = getpdfurl('', '2021-01-01', '2022-04-02', '年报', filter=False)
    getFundReportpdf(allpdf, fpath)