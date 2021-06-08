import requests
import pandas as pd
import re
import utils
from bs4 import SoupStrainer
from bs4 import BeautifulSoup

EXTRACTION_FILE_PATH='data/output.xlsx'

def crawl_summary(session, summary, coin):
    url = f'https://finance.yahoo.com/quote/{coin}?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='quote-summary')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        tr_data = soup.select('table tr')
        if tr_data:
            for tr in tr_data:
                td_data = tr.find_all('td')
                key = td_data[0].text
                value = td_data[1].text
                summary[key] = [value]

def crawl_stats(session, stats, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/key-statistics?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        tables = soup.find_all('div', class_=re.compile('^Fl.+?smartphone.+'))

        for table in tables:
            h2 = table.h2.text if table.h2 else None
            small_tables = table.find_all('div', class_=re.compile('^Pos.+?Mt.+'))
            if small_tables:
                for small_table in small_tables:
                    h3 = small_table.h3.text if small_table.h3 else None
                    tr = small_table.find_all('tr')
                    if tr:
                        for data in tr:
                            td = data.find_all('td')
                            col_1 = td[0].find('span').text if td[0].find('span') else None
                            col_2 = td[1].text
                            key = h2 if h2 else ''
                            key += f'|{h3}' if h3 else ''
                            key += f'|{col_1}' if col_1 else ''
                            stats[key] = [col_2]

def crawl_history(session, history, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/history?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('table', attrs={'data-test':'historical-prices'})
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        keys = [th.text for th in soup.select('thead tr th')]
        for tr in soup.select('tbody tr'):
            values = [td.text for td in tr.find_all('td')]
            if len(values) == len(keys):
                history.loc[len(history), keys] = values

def crawl_profile(session, profile, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/profile?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        info = soup.find('div', class_='asset-profile-container')
        
        if info:
            company_name = info.find('h3').text if info.find('h3') else None
            address = re.sub(r'(.+?)\d+\-.+', r'\1', info.select_one('p:nth-child(1)').text) if info.select_one('p:nth-child(1)') else None
            phone = info.find('a', attrs={'href': re.compile('^tel.+')}).text if info.find('a', attrs={'href': re.compile('^tel.+')}) else None
            website = info.find('a', attrs={'href': re.compile('^http.+')}).text if info.find('a', attrs={'href': re.compile('^http.+')}) else None
            profile['Company Name'] = [company_name]
            profile['Address'] = [address]
            profile['Phone'] = [phone]
            profile['Website'] = [website]
            more_span = info.select('.asset-profile-container p:nth-child(2) span')
            if more_span:
                count = 0
                key = None
                value = None
                for span in more_span:
                    if count % 2 == 0:
                        key = span.text
                    else:
                        value = span.text
                    if key and value:
                        profile[key] = [value]
                        key = None
                        value = None
                    count += 1

        desc = soup.find('div', class_='W(48%) smartphone_W(100%) Fl(start)') or soup.find('section', class_=re.compile('^quote-sub-section'))
        if desc:
            profile['Description'] = [desc.text.strip()]

        corporate = soup.find('section', class_=re.compile('.+?corporate-governance-container'))
        if corporate:
            h2 = corporate.h2.text if corporate.h2 else None
            profile[h2] = [corporate.div.text]

        map = soup.find('div', attrs={'data-test': 'yahoo-map'})
        if map:
            data_map = map['style']
            data_map = re.search(r'.+?url\((.+?)\).*', data_map)
            
            if data_map:
                profile['Map Image'] = [data_map.group(1)]

        info_more = soup.find('section', class_=re.compile('.+?quote-subsection.+'))
        if info_more:
            h3 = info_more.h3.text if info_more.h3 else None
            key_more = [f'{h3}|' + span.text for span in info_more.select('table thead th span')]
            tr_more = info_more.select('tbody tr')
            first = True
            for tr in tr_more:
                values = [td.text for td in tr.find_all('td')]
                if first:
                    profile.loc[0, key_more] = values
                    first = False
                else:
                    profile.loc[len(profile), key_more] = values

def crawl_financials(session, financials, type, coin):
    if type == 'is':
        url = f'https://finance.yahoo.com/quote/{coin}/financials?p={coin}'
    elif type == 'bs':
        url = f'https://finance.yahoo.com/quote/{coin}/balance-sheet?p={coin}'
    else:
        url = f'https://finance.yahoo.com/quote/{coin}/cash-flow?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        h3 = soup.h3.text if soup.h3 else None
        keys = [span.text for span in soup.select('div[class="D(tbhg)"] span')]
        rows = soup.find_all('div', attrs={'data-test':'fin-row'})
        if rows:
            for row in rows:
                values = []
                for data in row.find_all('div', class_= [re.compile('^D\(tbc\)'), re.compile('^Ta\(c\)')]):
                    span = data.find('span').text if data.find('span') else None
                    values.append(span)
                financials.loc[len(financials), keys] = values

def crawl_analysis(session, analysis, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/analysis?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        tables = soup.find_all('table')
        if tables:
            it = 0
            for table in tables:
                keys = []
                heads = table.select('thead tr th')
                first = True
                for head in heads:
                    if first == True:
                        keys.append(head.text)
                        first = False
                    else:
                        keys.append(head.text + f'_{it}')
                trs = table.select('tbody tr')
                i = 0
                for tr in trs:
                    data = [span.text for span in tr.find_all('td')]
                    analysis.loc[i, keys] = data
                    i += 1

            it += 1

def crawl_options(session, options, type, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/options?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        ops = soup.find_all('section', class_=re.compile('^Mt\(20px\)'))
        if ops:
            if type == 'calls':
                section = ops[0] 
            else:
                section = ops[1] 
        
            table = section.find('table')
            if table:
                key = [span.text for span in table.select('thead tr span')]
                trs = table.select('tbody tr')
                for tr in trs:
                    values = [data.text for data in tr.find_all('td')]
                    options.loc[len(options), key] = values
            
def crawl_major_holders(session, major_holders, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/holders?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        summary = soup.find('div', attrs={'data-test':'holder-summary'})
        summary_header = [summary.find('h5').text if summary.find('h5') else None]
        summary_value = [data.find_all('td')[0].text + '|' + data.find_all('td')[1].text for data in summary.find_all('tr')]
        major_holders[summary_header] = summary_value

        tops = soup.find_all('div', class_=re.compile('Mt\(25px\)'))
        for top in tops:
            h3 = top.find('h3').text if top.find('h3') else None
            keys = [f'{h3}|'+span.text for span in top.select('table thead th')]
            i = 0
            for tr in top.select('tbody tr'):
                values = [data.text for data in tr.find_all('td')]
                major_holders.loc[i, keys] = values
                i += 1
        
def crawl_insider_roster(session, insider_roster, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/insider-roster?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        keys = [th.text for th in soup.select('table thead th')]
        for row in soup.select('tbody tr'):
            values = [data.text for data in row.find_all('td')]
            insider_roster.loc[len(insider_roster), keys] = values

def crawl_insider_transactions(session, insider_transactions, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/insider-transactions?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        infos = soup.find_all('div', class_=re.compile('Mt\(25px\)'))
        if infos:
            it = 0
            first = True
            for info in infos:
                head_title = info.find('h3').text if info.find('h3') else None
                keys = []
                for th in info.select('table thead th'):
                    if first == True:
                        if head_title:
                            keys.append(f'{head_title}|' + th.text)
                        else:
                            keys.append(th.text)
                        first = False
                    else:
                        if head_title:
                            keys.append(f'{head_title}|' + th.text + f'_{it}')
                        else:
                            keys.append(th.text + f'_{it}')
                
                i = 0
                for row in info.select('table tbody tr'):
                    values = [td.text for td in row.find_all('td')]
                    insider_transactions.loc[i, keys] = values
                    i += 1
            it += 1
    
def crawl_sustainability(session, sustainability, coin):
    url = f'https://finance.yahoo.com/quote/{coin}/sustainability?p={coin}'
    r = session.get(url)
    if r.url == url:
        parse_only = SoupStrainer('div', id='Main')
        soup = BeautifulSoup(r.text, 'lxml', parse_only=parse_only)
        risk_rating = soup.find('div', class_=re.compile('^smartphone_Pt'))
        if risk_rating:
            total = risk_rating.find('div', class_=re.compile('^D\(ib\)'))
            total_data= total.find('div', class_=re.compile('^Pos\(r\)'))
            total_title = total_data.find_all('div')[0].text
            total_score = total_data.find_all('div')[1].find('div', class_=re.compile('Fz\(36px\)')).text
            sustainability[total_title] = [total_score]

            more_scores = risk_rating.find_all('div', class_=re.compile('Va\(t\) D\(ib\)'))
            i = 0
            for mscore in more_scores:
                title = mscore.find('div', class_='C($tertiaryColor) Fz(s)').text
                score = mscore.find('div', class_='D(ib) Fz(23px) smartphone_Fz(22px) Fw(600)').text
                sustainability[title] = [score]

            level = soup.find('div', class_='Mt(20px) smartphone_Px(20px)')
            title = level.h3.text if level.h3 else None
            score = level.find('div', class_='D(ib) Fz(36px) Fw(500)').text if level.find('div', class_='D(ib) Fz(36px) Fw(500)') else None
            sustainability[title] = [score]

def crawl(coin):

    logger = utils.create_logger(f'Crawling - {coin}', './logs/', True)
    writer = pd.ExcelWriter(EXTRACTION_FILE_PATH, engine='openpyxl')
    
    session = requests.Session()
    session.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }
    input = pd.DataFrame({'coin':[coin]})
    summary = pd.DataFrame()
    stats = pd.DataFrame()
    history = pd.DataFrame()
    profile = pd.DataFrame()
    financials_is = pd.DataFrame()
    financials_bs = pd.DataFrame()
    financials_cf = pd.DataFrame()
    analysis = pd.DataFrame()
    options_calls = pd.DataFrame()
    options_puts = pd.DataFrame()
    major_holders = pd.DataFrame()
    insider_roster = pd.DataFrame()
    insider_transactions = pd.DataFrame()
    sustainability = pd.DataFrame()

    try:
        logger.info('Begin to crawl Summary data')
        crawl_summary(session, summary, coin)
        if summary.empty:
            logger.info('Not found Summary data')
        else:
            logger.info('Crawled Summary data successful')
    except Exception as e:
        logger.error(e)

    try:
        logger.info('Begin to crawl Statistics data')
        crawl_stats(session, stats, coin)
        if stats.empty:
            logger.info('Not found Statistics data')
        else:
            logger.info('Crawled Statistics data successful')
    except Exception as e:
        logger.error(e)

    try:
        logger.info('Begin to crawl Historical data')
        crawl_history(session, history, coin)
        if history.empty:
            logger.info('Not found Historical data')
        else:
            logger.info('Crawled Historical data successful')
    except Exception as e:
        logger.error(e)

    try:
        logger.info('Begin to crawl Profile data')
        crawl_profile(session, profile, coin)
        if profile.empty:
            logger.info('Not found Profile data')
        else:
            logger.info('Crawled Profile data successful')
    except Exception as e:
        logger.error(e)


    try:
        logger.info('Begin to crawl Financials data.')
        crawl_financials(session, financials_is, 'is', coin)
        crawl_financials(session, financials_bs, 'bs', coin)
        crawl_financials(session, financials_cf, 'cf', coin)
        if financials_is.empty:
            logger.info('Not found Financials - Income Statement data')
        else:
            logger.info('Crawled Financials - Income Statement data successful')
        if financials_bs.empty:
            logger.info('Not found Financials - Balance Sheet data')
        else:
            logger.info('Crawled Financials - Balance Sheet data successful')
        if financials_cf.empty:
            logger.info('Not found Financials - Cash Flow data')
        else:
            logger.info('Crawled Financials - Cash Flow data successful')
    except Exception as e:
        logger.error(e)

    try:
        logger.info('Begin to crawl Analysis data')
        crawl_analysis(session, analysis, coin)
        if analysis.empty:
            logger.info('Not found Analysis data')
        else:
            logger.info('Crawled Analysis data successful')
    except Exception as e:
        logger.error(e)   

    try:
        logger.info('Begin to crawl Options data')
        crawl_options(session, options_calls, 'calls', coin)
        crawl_options(session, options_puts, 'puts', coin)
        if options_calls.empty:
            logger.info('Not found Options Calls data')
        else:
            logger.info('Crawled Options Calls data successful')
        
        if options_puts.empty:
            logger.info('Not found Options Puts data')
        else:
            logger.info('Crawled Options Puts data successful')
    except Exception as e:
        logger.error(e)   
    
    try:
        logger.info('Begin to crawl Major Holders data')
        crawl_major_holders(session, major_holders, coin)
        crawl_insider_roster(session, insider_roster, coin)
        crawl_insider_transactions(session, insider_transactions, coin)
        if major_holders.empty:
            logger.info('Not found Major Holders data')
        else:
            logger.info('Crawled Major Holders data successful')

        if insider_roster.empty:
            logger.info('Not found Insider Roster data')
        else:
            logger.info('Crawled Insider Roster data successful')

        if insider_transactions.empty:
            logger.info('Not found Insider Transactions data')
        else:
            logger.info('Crawled Insider Transactions data successful')
    except Exception as e:
        logger.error(e)  
    
    
    try:
        logger.info('Begin to crawl Sustainability data.')
        crawl_sustainability(session, sustainability, coin)
        if sustainability.empty:
            logger.info('Not found Sustainability data')
        else:
            logger.info('Crawled Sustainability data successful')
    except Exception as e:
        logger.error(e)  
    
    input.to_excel(writer, sheet_name='Input', index=False, header=None)
    summary.to_excel(writer, sheet_name='Summary', index=False)
    stats.to_excel(writer, sheet_name='Statistics', index=False)
    history.to_excel(writer, sheet_name='Historical', index=False)
    profile.to_excel(writer, sheet_name='Profile', index=False)
    financials_is.to_excel(writer, sheet_name='Fin - Income Statement', index=False)
    financials_bs.to_excel(writer, sheet_name='Fin - Balance Sheet', index=False)
    financials_cf.to_excel(writer, sheet_name='Fin - Cash Flow', index=False)
    analysis.to_excel(writer, sheet_name='Analysis', index=False)
    options_calls.to_excel(writer, sheet_name='Options Calls', index=False)
    options_puts.to_excel(writer, sheet_name='Options Puts', index=False)
    major_holders.to_excel(writer, sheet_name='Major Holders', index=False)
    insider_roster.to_excel(writer, sheet_name='Insider Roster', index=False)
    insider_transactions.to_excel(writer, sheet_name='Insider Transactions', index=False)
    sustainability.to_excel(writer, sheet_name='Sustainability', index=False)
    writer.save
    writer.close()
    

if __name__ == '__main__':
    df = pd.read_excel(EXTRACTION_FILE_PATH,engine='openpyxl', header=None, sheet_name='Input')
    if df.empty:
        print("+++++++++++ERROR: PLEASE FILL DATA IN INPUT SHEET+++++++++++")
    else:
        crawl(df.values[0][0])