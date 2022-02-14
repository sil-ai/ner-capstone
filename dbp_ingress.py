import argparse
import os

import string
import numpy as np
import requests
import pandas as pd
import urllib3
import certifi
import json  
from difflib import SequenceMatcher
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
stopwords = set(stopwords.words('english'))

def extract_api_data(url, DBP_KEY):
    '''
    Extract data DBP API.
    Login https://www.digitalbibleplatform.com/site/wp-login.php?loggedout=true
    API docs https://www.digitalbibleplatform.com/developer/

    Args:
        url - the URL for the desired API
    Returns: 
        a pandas dataframe
    '''

    #DBP_KEY = os.environ.get('DBP_KEY')
    response = requests.get(url + '?v=4&key={}'.format(DBP_KEY))
    num_pages = response.json()['meta']['pagination']['last_page']


    if response.ok:
        # Initialize dataframe
        df = pd.DataFrame()

        # Loop through each page to get all Bible records

        for page in range(1, num_pages + 1):
        
            temp_df = pd.DataFrame(requests.get(url + '?v=4&key={}&page={}'.format(DBP_KEY, page)).json()['data'])
            df = df.append(temp_df)
        df = df.reset_index(drop=True)

        return df
    else:
        raise Exception('Check API key and/or URL')

def extract_video_api(url, DBP_KEY):
    response = requests.get(url + '&v=4&key={}'.format(DBP_KEY))
    num_pages = response.json()['meta']['pagination']['last_page']

    if response.ok:
        # Initialize dataframe
        df = pd.DataFrame()

        # Loop through each page to get all Bible records

        for page in range(1, num_pages + 1):
        
            temp_df = pd.DataFrame(requests.get(url + '&v=4&key={}&page={}'.format(DBP_KEY, page)).json()['data'])
            df = df.append(temp_df)
        df = df.reset_index(drop=True)

        


        return df
    else:
        raise Exception('Check API key and/or URL')
    
def clean_dataframe(df, DBP_KEY, organization_warehouse, copyright_df, publishers_df, unique_orgs):

    
    # Break out contents of filesets into two columns
    df[['dbp-prod', 'dbp-vid']] = df['filesets'].apply(pd.Series)

    # Explode columns with different types of metadata 
    df = df.explode('dbp-prod')
    df = df.explode('dbp-vid').reset_index(drop=True)
    
    # Break out contents of dictionary columns
    df[df['dbp-prod'].apply(pd.Series).columns] = df['dbp-prod'].apply(pd.Series)

    df = df.rename(columns={'id': 'id_prod', 'type': 'type_prod', 'size':'size_prod', 'stock_no':'stock_no_prod', 'volume':'volume_prod'})
    # df[['0', 'bitrate','codec','container','id_prod','size_prod','sku','stock_no','timing_est_err','type_prod','volume_prod']] = df['dbp-prod'].apply(pd.Series)
    df[df['dbp-vid'].apply(pd.Series).columns] = df['dbp-vid'].apply(pd.Series)
    
    df = df.rename(columns={'id': 'id_vid', 'type': 'type_vid', 'size':'size_vid', 'stock_no':'stock_no_vid', 'volume':'volume_vid'})

    # Drop unnecessary columns
    df = df[['abbr', 'name', 'vname',  'language', 'autonym', 'language_id',
             'iso',  'date','id_prod','type_prod','size_prod','stock_no_prod','volume_prod',
              'bitrate', 'codec','container', 'timing_est_err', 'id_vid',
             'size_vid', 'stock_no_vid', 'type_vid','volume_vid',]]
    

    cleaned_df_columns = ['abbr', 'name', 'vname', 'language', 'autonym', 'language_id', 'iso', 'date', 'bitrate', 'codec', 'container', 'id_prod', 'size_prod', 
                        	'stock_no_prod', 'timing_est_err', 'type_prod', 'volume_prod', 'id_vid', 'size_vid', 'stock_no_vid', 'type_vid', 'volume_vid', 'RightsholderOrganizationId',
                            'copyright_statement', 'publishers_id']
    
    

    #split type_prod in order to see text vs multimedia
    df = df[df['type_prod'] != 'text_format']

    #drop the rows with 16kbps since all products have 64kbps
    df = df[df['bitrate'] != '16kbps']   

    text = []
    usx = []
    for z in df.index:
        abbr = df.loc[z,'abbr']
        bitrate = df.loc[z, 'type_prod']
        if bitrate == 'text_plain':
            text.append(abbr)
        elif bitrate == 'text_usx':
            usx.append(abbr)  

    text = list(set(text))
    usx = list(set(usx))

    usx_list = [i for i in text if i in usx]

    for k in df.index:
        abb = df.loc[k,'abbr']
        tex = df.loc[k, 'type_prod']
        for j in usx_list:
            if abb == j and tex == 'text_usx':
                df = df.drop([k])

    for i in df.index:
        prod_type = df.loc[i, 'type_prod']
        if prod_type == 'text_plain' or prod_type == 'text_usx':
            df.loc[i, 'type_prod'] = 'text'
        elif prod_type == 'audio_stream':
            df.loc[i, 'type_prod'] = 'audio'
        elif prod_type == 'audio_drama_stream':
            df.loc[i, 'type_prod'] = 'audio_drama'
 
    #get rid of duplicated if same product has text
    df = df.drop_duplicates(subset=['abbr', 'type_prod'], keep='last')
    df = df.reset_index(drop=True)

    # Remove year to match with organization table
    
    #df['name'] = df['name'].apply(lambda x: ''.join([i for i in x if not i.isdigit()]) if(x) != np.nan else x)
    
    #df['name'] = df['name'].str.strip()
    # Make Wycliffe consistent 

    df = df.merge((organization_warehouse[['abbr', 'id']]), on= 'abbr', how='left')

    def clean_string(text):
        text = ''.join([word for word in text if not word.isdigit()])
        text = ''.join([word for word in text if word not in string.punctuation])
        text = text.lower()
        text = ''.join([word for word in text.split() if word not in stopwords])
        #text = text.replace('inc', '')
        text = text.strip()

        return text

    def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

    df['orgs_id'] = pd.Series()
    for k in df.index:
        try:
            name = df.loc[k,'name']
            name = clean_string(name)
            for j in unique_orgs.index:
                orgs_name = unique_orgs.loc[j, 'slug']
                orgs_name = clean_string(str(orgs_name))
                percen = similar(name, orgs_name)
                
                if percen > 0.9:
                    df['orgs_id'][k] = str(unique_orgs['id'][j])
                else:
                    pass

        except Exception:
            pass

    df['orgs_id'].fillna("None", inplace = True)

    for q in df.index:
            orgs = df.loc[q,'orgs_id']
            ids = df.loc[q, 'id']
            ids = str(ids)
            ids = ids.replace('{', '')
            ids = ids.replace('}','')
            ids = ids.split(',')
            if orgs == "None" and len(ids) == 1:
                df.loc[q, 'orgs_id'] = df.loc[q, 'id']
            elif orgs == "None" and len(ids) > 1:
                df.loc[q, 'orgs_id'] = str(ids[-1])
        

    df = df.drop(['id'], axis=1)
    df = df.rename(columns = {'orgs_id': 'RightsholderOrganizationId'})

    copyright_df = copyright_df.rename(columns = {'id': 'abbr'})
    df = df.merge((copyright_df[['abbr', 'copyright_statement']]), on= 'abbr', how='left')

    publishers_df = publishers_df.rename(columns={'id':'publishers_id'})
    df = df.merge((publishers_df[['abbr', 'publishers_id']]), on= 'abbr', how='left')


    df = df.drop_duplicates(subset=['abbr', 'type_prod'], keep='first')
    df = df.reset_index(drop=True)

    #df["publishers_id"] = df["publishers_id"].astype(str).astype(int)
    #df["RightsholderOrganizationId"] = df["RightsholderOrganizationId"].astype(int)

    

    if len(cleaned_df_columns) == len(df.columns):
        # Confirm the column headers are as expected
        if sorted(cleaned_df_columns) == sorted(df.columns):
            return df
        else:
            raise Exception(
                'Columns have changed in the cleaned dataframe. Verify column header changes.')
    else:
        raise Exception(
            'The number of columns have changed in the cleaned dataframe. Verify the column headers.')

def bibleChapterLabels(df, df_video):
    

    df['chapter_ids'] = pd.Series()
    df['chapter_count'] = pd.Series()
    df['ScriptureSetCode'] = pd.Series()
    df['Medium'] = pd.Series()
    df['Mode'] = pd.Series()
    df['URL'] = pd.Series()

    #for accessing the API
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    #loop that will access each entry to find the exact chapters not available in the main API
    for i in df.index:
        
        
            ######Can you do this in another way than a try/except? Anyway you could do this as an if/then? Maybe: if chapter_api.ok:
        try:
            if len(df.loc[i, 'id_prod']) > 6:
                prod = df.loc[i, 'id_prod']
                DBP_KEY = os.environ.get('DBP_KEY')
                chap_api = http.request('GET', 'https://4.dbt.io/api/bibles/filesets/{}/?v=4&key={}'.format(prod, DBP_KEY))
                data = json.loads(chap_api.data.decode('utf-8'))
                df_chap_api = pd.json_normalize(data)
                prods = []

            #printing book code with each chapter number

                for prod in range(len(df_chap_api['data'][0])):
                    if df_chap_api['data'][0][prod]['chapter_start']==0:
                        continue 
                    elif len(str(df_chap_api['data'][0][prod]['chapter_start']))==1:
                        prods.append(df_chap_api['data'][0][prod]['book_id']+'00'+str(df_chap_api['data'][0][prod]['chapter_start']))
                    elif len(str(df_chap_api['data'][0][prod]['chapter_start']))==2:
                        prods.append(df_chap_api['data'][0][prod]['book_id']+'0'+str(df_chap_api['data'][0][prod]['chapter_start']))
                df['chapter_ids'][i] = ', '.join(map(str, prods))
                df['chapter_count'][i] = len(prods)

                

            else:
                abbr = df.loc[i, 'abbr']
                DBP_KEY = os.environ.get('DBP_KEY')
                chapter_api = http.request('GET', 'https://4.dbt.io/api/bibles/{}?v=4&key={}'.format(abbr, DBP_KEY))
                data = json.loads(chapter_api.data.decode('utf-8'))
                df_chapter_api = pd.json_normalize(data)
                book_data = df_chapter_api.loc[0, 'data.books']
                df_chapters = pd.json_normalize(book_data)
                df_chapters['book_labels'] = pd.Series()
                x = []

            #printing book code with each chapter number

                for q in df_chapters.index:
                    book = df_chapters.loc[q,"book_id"]
                    for j in range(len(df_chapters.chapters[q])):
                        if df_chapters.chapters[q][j] == 0:
                            continue
                        elif len(str(df_chapters.chapters[q][j])) == 1: 
                            x.append(book+"00"+str(df_chapters.chapters[q][j]))
                        elif len(str(df_chapters.chapters[q][j])) == 2:
                            x.append(book+"0"+str(df_chapters.chapters[q][j]))
                        else:
                            x.append(book+str(df_chapters.chapters[q][j]))
                df['chapter_ids'][i] = ', '.join(map(str, x))
                df['chapter_count'][i] = len(x)
     
            
            if df['chapter_count'][i] == 260:
                df['ScriptureSetCode'][i] = 'NEW COMPLETE'
            elif df['chapter_count'][i] == 1189:
                df['ScriptureSetCode'][i] = 'BIB COMPLETE'
            elif df['chapter_count'][i] > 1189:
                df['ScriptureSetCode'][i] = 'BIB COMPLETE, DEC SELECT'
            elif df['size_prod'][i] == 'NTP':
                df['ScriptureSetCode'][i] = 'NT SELECT'
            elif df['size_prod'][i] == 'NT':
                df['ScriptureSetCode'][i] = 'NEW COMPLETE'
            elif df['size_prod'][i] == 'C':
                df['ScriptureSetCode'][i] = 'BIB COMPLETE'
            elif df['size_prod'][i] == 'NTPOTP':
                df['ScriptureSetCode'][i] = 'OT SELECT, NT SELECT'
            elif df['size_prod'][i] == 'NTOTP':
                df['ScriptureSetCode'][i] = 'OLD SELECT, NEW COMPLETE'
            elif df['size_prod'][i] == 'OTNTP':
                df['ScriptureSetCode'][i] = 'OLD COMPLETE, NEW COMPLETE'
            elif df['size_prod'][i] == 'OTP':
                df['ScriptureSetCode'][i] = 'OT SELECT'
            elif df['size_prod'][i] == 'OT':
                df['ScriptureSetCode'][i] = 'OLD COMPLETE'  
            elif df['size_prod'][i] == 'P':
                df['ScriptureSetCode'][i] = 'SELECT'
            elif df['size_prod'][i] == 'S':
                df['ScriptureSetCode'][i] = 'BIB STORY'
            else: 
                df['ScriptureSetCode'][i] = "NULL"
            
            
            if df['type_prod'][i] == 'text':
                df['Medium'][i] = 'DIGITAL'
                df['Mode'][i] = 'TE'
            elif df['type_prod'][i] == 'audio':
                df['Medium'][i] = 'DIGITAL'
                df['Mode'][i] = 'AU'
            elif df['type_prod'][i] == 'audio_drama':
                df['Medium'][i] = 'DIGITAL'
                df['Mode'][i] = 'AU'
            else:
                pass

            abb = df.loc[i,'abbr']
            url = 'https://live.bible.is/bible/' + abb
            df.loc[i, 'URL'] = url 
        
        except Exception:
                pass

        

    #create dataframe for video products and append to products df
    
    df_vid = df[['abbr', 'name', 'vname', 'language', 'autonym', 'language_id', 'iso','date', 'id_vid','size_vid','stock_no_vid','type_vid','volume_vid','RightsholderOrganizationId','copyright_statement','publishers_id', 'chapter_ids', 'chapter_count', 'ScriptureSetCode', 'Medium','Mode']]
    df_vid = df_vid.dropna(subset=['id_vid'])
    df_vid['abbr'] = df_vid['id_vid'].str[:6]
    
    for product in df_vid.index:
        
        try:
            abb = df_vid.loc[product, 'id_vid']
            DBP_KEY = os.environ.get('DBP_KEY')
            chap_api = http.request('GET', 'https://4.dbt.io/api/bibles/filesets/{}/?v=4&key={}'.format(abb, DBP_KEY))
            data = json.loads(chap_api.data.decode('utf-8'))
            df_chap_api = pd.json_normalize(data)
            chaps = []

            #printing book code with each chapter number
        

            for lab in range(len(df_chap_api['data'][0])):
                if df_chap_api['data'][0][lab]['chapter_start']==0:
                    continue 
                elif len(str(df_chap_api['data'][0][lab]['chapter_start']))==1:
                    chaps.append(df_chap_api['data'][0][lab]['book_id']+'00'+str(df_chap_api['data'][0][lab]['chapter_start']))
                elif len(str(df_chap_api['data'][0][lab]['chapter_start']))==2:
                    chaps.append(df_chap_api['data'][0][lab]['book_id']+'0'+str(df_chap_api['data'][0][lab]['chapter_start']))
            df_vid['chapter_ids'][i] = ', '.join(map(str, chaps))
            df_vid['chapter_count'][i] = len(chaps)

        except Exception:
            pass

    #df_vid = df_vid.merge((df_video[['abbr', 'name', 'vname', 'language', 'autonym', 'language_id', 'iso','date']]), on= 'abbr', how='left')
    df_vid['type_vid'] = df_vid['type_vid'].str[:5]
    df_vid = df_vid.rename(columns={'id_vid': 'id_prod', 'size_vid': 'size_prod', 'stock_no_vid':'stock_no_prod', 'type_vid':'type_prod', 'volume_vid':'volume_prod' })
    df_vid.loc[:,'Medium'] = 'DIGITAL'
    df_vid.loc[:,'Mode'] = 'VI'
    df_vid = df_vid.drop_duplicates(subset=['abbr', 'id_prod'], keep='first')
    for vid in df_vid.index:
        if df_vid['chapter_count'][vid] == 260:
            df_vid['ScriptureSetCode'][vid] = 'NEW COMPLETE'
        elif df_vid['chapter_count'][vid] == 1189:
            df_vid['ScriptureSetCode'][vid] = 'BIB COMPLETE'
        elif df_vid['chapter_count'][vid] > 1189:
            df_vid['ScriptureSetCode'][vid] = 'BIB COMPLETE, DEC SELECT'
        elif df_vid['size_prod'][vid] == 'NTP':
            df_vid['ScriptureSetCode'][vid] = 'NT SELECT'
        elif df_vid['size_prod'][vid] == 'NT':
            df_vid['ScriptureSetCode'][vid] = 'NEW COMPLETE'
        elif df_vid['size_prod'][vid] == 'C':
            df_vid['ScriptureSetCode'][vid] = 'BIB COMPLETE'
        elif df_vid['size_prod'][vid] == 'NTPOTP':
            df_vid['ScriptureSetCode'][vid] = 'OT SELECT, NT SELECT'
        elif df_vid['size_prod'][vid] == 'NTOTP':
            df_vid['ScriptureSetCode'][vid] = 'OLD SELECT, NEW COMPLETE'
        elif df_vid['size_prod'][vid] == 'OTNTP':
            df_vid['ScriptureSetCode'][vid] = 'OLD COMPLETE, NEW COMPLETE'
        elif df_vid['size_prod'][vid] == 'OTP':
            df_vid['ScriptureSetCode'][vid] = 'OT SELECT'
        elif df_vid['size_prod'][vid] == 'OT':
            df_vid['ScriptureSetCode'][vid] = 'OLD COMPLETE'  
        elif df_vid['size_prod'][vid] == 'P':
            df_vid['ScriptureSetCode'][vid] = 'SELECT'
        elif df_vid['size_prod'][vid] == 'S':
            df_vid['ScriptureSetCode'][vid] = 'BIB STORY'
        else: 
            df_vid['ScriptureSetCode'][vid] = "NULL"

    df = df.append(df_vid,ignore_index=True)
    df = df.sort_values(by=['abbr'])
    #creating ScriptureSetCode, Medium and Mode collumns
    

    dict = {'abbr': 'FCBHId', 'iso': 'ISO', 'date': 'PublicationYear', 'publishers_id':'PublisherOrganizationId',
            'chapter_ids':'Chapter_Ids'}
  

    df.rename(columns=dict,inplace=True)
    #creating ScriptureSetCode, Medium and Mode collumns

    

    df = df.drop(columns = ['bitrate', 'codec', 'container', "timing_est_err", 'id_vid', 'size_vid', 'stock_no_vid', 'type_vid',
                        'volume_vid'], axis=1)
        
    df = df[df.id_prod.notnull()]
    df['FCBHId'] = df['id_prod']
    df = df[df['type_prod'] != 'video']
    df = df.drop(['stock_no_prod'], axis=1)

    return df


def orgs_json(df, DBP_KEY, publishers_df):
    df_orgs = pd.DataFrame(columns=['abbr', 'id'])
    
    #df_product_org = pd.DataFrame(columns=['id', 'orgs_id', 'name'])
    for i in df.index:
        try:
            record = df.loc[i, 'abbr']
            url = "https://4.dbt.io/api/bibles/{}/copyright?&v=4&key={}".format(record, DBP_KEY)
            data = pd.DataFrame(requests.get(url).json())
            orgs_list = []
            for k in data.index:
                #id_data = data.loc[k, 'id']
                for j in range(len(data['copyright'][k]['organizations'])):  
                    org = data['copyright'][k]['organizations'][j]['id']
                    orgs_list.append(org)
            df_orgs.loc[i, 'abbr'] = record
            df_orgs.loc[i, 'id'] = set(orgs_list)



        except Exception:
            pass
    
    df_unique_orgs = pd.DataFrame()
    org_unique_lists = []
    for record in df['abbr'].unique():
        url = "https://4.dbt.io/api/bibles/{}/copyright?&v=4&key={}".format(record, DBP_KEY)
        data = pd.DataFrame(requests.get(url).json())
        for j in range(len(data)):
            if data['copyright'][j] is not None: 
                orgs = data['copyright'][j]['organizations'][0]
                org_unique_lists.append(orgs)
          
            else:
                pass

    df_unique_orgs = pd.DataFrame(org_unique_lists)
    df_unique_orgs = df_unique_orgs.append(publishers_df)
    df_unique_orgs = df_unique_orgs.drop_duplicates('id', keep='last')
    df_unique_orgs = df_unique_orgs[['id', 'slug']]
    df_unique_orgs['slug'] = df_unique_orgs['slug'].astype(str).apply(lambda x: x.replace('-', ' ').title())

    return df_orgs, df_unique_orgs

def copyright_publisher(df, DBP_KEY):
  # Copyright statement 
  copyright_df = pd.DataFrame()
  for record in df['abbr'].unique():
    try:
        url = "https://4.dbt.io/api/bibles/{}/copyright?&v=4&key={}".format(record, DBP_KEY)
        data = pd.DataFrame(requests.get(url).json())
        data['copyright_statement'] = data['copyright'].apply(lambda x: x['copyright'])
        copyright_df = copyright_df.append(data)
    except Exception:
        pass
# Publishers
  publishers_df = pd.DataFrame(columns = ['abbr', 'org_list', 'slug'])
  
  for record in df['abbr'].unique():
    try:
        url = "https://4.dbt.io/api/bibles/{}?v=4&key={}".format(record, DBP_KEY)
        publishers_list = requests.get(url).json()['data']['publishers']
        publishers_df.loc[record, 'abbr'] = record
        publishers_df.loc[record, 'id'] = publishers_list[0]['id']
        publishers_df.loc[record, 'slug'] = publishers_list[0]['slug']
        
    except Exception:
        pass

  return copyright_df, publishers_df

def main():
    # Command line arguments
    parser = argparse.ArgumentParser(
        description='Process the GBC data and combine to form a single file.')
    parser.add_argument('outDir', type=str,
                        help='Output directory for the processed data')
    args = parser.parse_args()

    df = extract_api_data('https://4.dbt.io/api/bibles/', os.environ.get('DBP_KEY'))

    df_video = extract_video_api('https://4.dbt.io/api/bibles?&media=video_stream', os.environ.get('DBP_KEY'))
    
    copyright_df, publishers_df = copyright_publisher(df, os.environ.get('DBP_KEY'))

    organization_warehouse, unique_orgs = orgs_json(df, os.environ.get('DBP_KEY'), publishers_df)

    df_clean = clean_dataframe(df,os.environ.get('DBP_KEY'), organization_warehouse, copyright_df, publishers_df, unique_orgs)

    

    fcbh_df = bibleChapterLabels(df_clean, df_video)
    # Output files for data warehouse
    fcbh_df['SourceDate'] = pd.to_datetime('today').strftime('%m-%d-%Y')
    fcbh_df['SourceDate'] = fcbh_df['SourceDate'].astype(str)


    #organization_warehouse.to_csv(os.path.join(args.outDir, 'org_warehouse_df_test1.csv'), index=False, line_terminator='\r\n')
    
    #orgs_list.to_csv(os.path.join(args.outDir, 'organization_df.csv'), index=False, line_terminator='\r\n')
    unique_orgs.to_csv(os.path.join(args.outDir, 'OrganizationFCBH.csv'), index=False, line_terminator='\r\n')
    fcbh_df.to_csv(os.path.join(args.outDir, 'CompletedProductFCBH.csv'), index=False, line_terminator='\r\n')
    publishers_df.to_csv(os.path.join(args.outDir, 'publishers_df_test1.csv'), index=False, line_terminator='\r\n')
    print('DBP Ingress Completed Successfully!')


if __name__ == "__main__":
    main()