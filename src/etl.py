from py7zr import unpack_7zarchive
import shutil
import pandas as pd
from lxml import etree
import lxml
from copy import deepcopy
import glob

user_edits = {}
revert_pairs = []

mutual_revert_pairs = []
mutual_revert_users = []

def unzip(fp,output):
    shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
    shutil.unpack_archive(fp, output)

def fast_iter(context):
    '''loops through an XML object, and writes 1000 page elements per file.'''
    page_num = 1
    
    # create an empty tree to add XML elements to ('pages')
    # and a new file to write to.
    fh = make_tmpfile(page_num)
    tree = etree.ElementTree()
    root = etree.Element("wikimedia")
    
    # loop through the large XML tree (streaming)
    for event, elem in context:
        
        # After 1000 pages, write the tree to the XML file
        # and reset the tree / create a new file.
        if page_num % 10 == 0:
            
            tree._setroot(root)
            try:
                fh.write(etree.tostring(tree).decode('utf-8'))
                fh.close()
            except:
                print('oops')
        
            fh = make_tmpfile(page_num)
            tree = etree.ElementTree()
            root = etree.Element("wikimedia")
        
        # add the 'page' element to the small tree
        root.append(deepcopy(elem))
        page_num += 1
        

        # release uneeded XML from memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
            
    del context

def make_tmpfile(pagenum, dir='newdata'):
    '''returns a file object for a small chunk file; must close it yourself'''
    import os
    if not os.path.exists(dir):
        os.mkdir(dir)
        
    fp = os.path.join(dir, 'chunk_%d.xml' % pagenum)
    return  open(fp, mode='w')

def parse_unzipped(fp1):
    context = etree.iterparse(fp1, tag='{http://www.mediawiki.org/xml/export-0.10/}page', encoding='utf-8')
    fast_iter(context)

def run_file(file_path):
    try:
        et = etree.parse(file_path)
    except:
        print('blank doc')
        return
    root = et.getroot()
    nsmap = {'ns': 'http://www.mediawiki.org/xml/export-0.10/'}
    pages_dicts = []
    fixed_pages_dicts =[]

    for pages in root.findall('ns:page', nsmap):
        current_title = pages.find('ns:title',nsmap).text
        current_page_id = pages.find('ns:id',nsmap).text
        
        current_page_dicts = []
        
        for revision in pages.findall('ns:revision',nsmap):
            revision_id = revision.find('ns:id',nsmap).text
            revision_parentid = revision.find('ns:parentid',nsmap)
            if revision_parentid != None:
                revision_parentid = revision_parentid.text
            else:
                revision_parentid = ''
            revision_timestamp = revision.find('ns:timestamp',nsmap).text

            contributor = revision.find('ns:contributor',nsmap)
            contributor_username = contributor.find('ns:username',nsmap)
            contriburtor_id = contributor.find('ns:id',nsmap)
            contriburtor_ip = contributor.find('ns:ip',nsmap)
            text = revision.find('ns:text',nsmap).text
            #catch lack of username or ip
            if contributor_username != None:
                contributor_username = contributor_username.text
            else:
                contributor_username = ''
            if contriburtor_id != None:
                contriburtor_id = contriburtor_id.text
            else:
                contriburtor_id = ''
            if contriburtor_ip != None:
                contriburtor_ip = contriburtor_ip.text
            else:
                contriburtor_ip = ''
            temp_dict = {'text':text,'page_title':current_title,'page_id':current_page_id,'revision_id':revision_id
                         ,'revision_parentid':revision_parentid,'revision_timestamp':revision_timestamp,
                         'contributor_username':contributor_username,'contributor_id':contriburtor_id,'contributor_ip':contriburtor_ip}
            current_page_dicts.append(temp_dict)
            pages_dicts.append(temp_dict)
            
            
        counter = 0
        revision_ids = {}
        revision_texts = {}
        temp_fixed_pages_dicts = []
        for i in range(len(current_page_dicts)):
            entry = current_page_dicts[i]
            contributor = ''
            current_title = entry['page_title']
            
            #identify correct contributor
            if entry['contributor_username']!='':
                contributor = entry['contributor_username']
            else:
                contributor = entry['contributor_ip']
                
            #first line
            if i == 0:
                temp_fixed_dict = {'time':entry['revision_timestamp'],'revert':0,'edit_num':counter,'contributor':contributor,'page_title':current_title}
                revision_ids[entry['revision_id']] = counter
                revision_texts[entry['text']] = counter
                counter +=1
                temp_fixed_pages_dicts.append(temp_fixed_dict)
                continue
                
#             previous_entry = current_page_dicts[i-1]
            
            #check if revert or not
            elif entry['text'] not in revision_texts:
                temp_fixed_dict = {'time':entry['revision_timestamp'],'revert':0,'edit_num':counter,'contributor':contributor,'page_title':current_title}
                revision_ids[entry['revision_id']] = counter
                revision_texts[entry['text']] = counter
                counter += 1
                temp_fixed_pages_dicts.append(temp_fixed_dict)
            else:
#                 print("found revert")
#                 print(i)
#                 print(pd.DataFrame(current_page_dicts[0:20]))
                #find which one it was reverted to
                if entry['revision_parentid'] == '':
                    continue
                if entry['revision_parentid'] not in revision_ids:
                    continue
                reverted_edit_num = revision_texts[entry['text']]
                temp_fixed_dict = {'time':entry['revision_timestamp'],'revert':1,'edit_num':reverted_edit_num,'contributor':contributor,'page_title':current_title}
                temp_fixed_pages_dicts.append(temp_fixed_dict)
                
        temp_fixed_pages_dicts.reverse()
        fixed_pages_dicts = fixed_pages_dicts + temp_fixed_pages_dicts
        
            
    f = open("newtext.txt","a")
    current_article = fixed_pages_dicts[0]['page_title']
    current_article = current_article.replace(" ", "_")
    f.write(current_article+'\n')
    write_next= False
    for value in fixed_pages_dicts:
#         if current_article != value['page_title'].replace(" ", "_"):
#             current_article = value['page_title']
#             current_article=current_article.replace(" ", "_")
#             f.write(current_article+'\n')
#             current_article = value['page_title']
        
        if write_next:
            write_next = False
            current_article = value['page_title'].replace(" ","_")
            f.write(current_article+'\n')
        if value['edit_num'] == 0:
            write_next = True
        try:
            f.write(value['time']+' '+str(value['revert'])+' '+str(value['edit_num'])+' '+value['contributor']+'\n')
        except:
            print(value)
    f.close()
    
    
#     df = pd.DataFrame(pages_dicts)
#     df['revision_timestamp'] =  pd.to_datetime(df['revision_timestamp'])
#     df[['page_title','contributor_username','contributor_ip']] = df[['page_title','contributor_username','contributor_ip']].astype(str)
#     df[['page_id','revision_id']] = df[['page_id','revision_id']].astype(int)
#     conn = sqlite3.connect('test2.db')
#     df.to_sql('temp', conn, if_exists='append', index=False)
    
#     df = pd.DataFrame(fixed_pages_dicts)
#     conn1 = sqlite3.connect('fixed_test2.db')
#     df.to_sql('temp', conn1, if_exists='append', index=False)
    
#     print(pd.read_sql('select * from temp', conn))


def convert_to_light():
    vals = glob.glob("newdata/*")
    for file in range(len(vals)):
        print('running'+str(file)+'/'+str(len(vals)))
        run_file(vals[file])

def getLine(label):
    global lineLabels
    for line, ll in reversed(list(enumerate(lineLabels))):
        if lineLabels[line] == label:
            return line

def readPage(inputParts):
    global user_edits
    global revert_pairs
   
    global revert
    for i in range(len(inputParts)):
        try:
            if inputParts[i][3] not in user_edits:
                user_edits[inputParts[i][3]] = 1
            else:
                user_edits[inputParts[i][3]] = user_edits[inputParts[i][3]] + 1
        except:
            continue

        if inputParts[i][1] == '1':
           #the found line is the version i-1 equal to this version j, and the revert is assumed to be between the author of i, and j
            
            #get reverted to line
            author = ''
            line = -1
            for x in range(i-1,-1,-1):
#             for x in range(0,i):
                try:
                    if inputParts[x][2] == inputParts[i][2] and inputParts[x][1] == '0':
                        author = inputParts[x][3]
                        line = x
                        break
                except:
                    break
            
            
           #ignore cases when i-1, and i are equal (consecutive versions)
            if line == i - 1:
                continue
            
            
                 
            revertedU = author
            revertingU = inputParts[i][3]
            if revertedU == revertingU:
                continue
            pair = revertedU +"~!~" + revertingU 
            if pair not in revert_pairs:
                revert_pairs.append(pair)
    
def getMutual():
    global revert_pairs
    global mutual_revert_users

    for pair in revert_pairs:
        parts = pair.split("~!~")
        if parts[1] + "~!~" + parts[0] in revert_pairs:
            sorted_pair = ""
            if parts[0] < parts[1]:
                sorted_pair = parts[0] + "~!~" + parts[1]
            else:
                sorted_pair = parts[1] + "~!~" + parts[0]
            mutual_revert_pairs.append(sorted_pair)
            if parts[1] not in mutual_revert_users:
                mutual_revert_users.append(parts[1])
            if parts[0] not in mutual_revert_users:
                mutual_revert_users.append(parts[0])


def calculate_M():

    m_values = {}
    page_edits = []
    current_page_name = ''

    user_edits = {}
    revert_pairs = []

    mutual_revert_pairs = []
    mutual_revert_users = []
    results = {}


    df2 = pd.DataFrame(columns=['page_name','score'])
    i = 0
    for ln in open('newtext.txt','r'):
        line = ln.strip().split(' ')
        if len(line) == 1 and len(page_edits) != 0:
            #reset all values
            user_edits = {}
            revert_pairs = []
            mutual_revert_pairs = []
            mutual_revert_users = []
            page_edits.reverse()
            
            
            readPage(page_edits)
            
    #         if(len(revert_pairs) > 0):
    #             print(current_page_name)
    #             print(revert_pairs)
                
    #             print()
            
            
            getMutual()
            
            score = 0
            max_score =0 
            for pair in list(set(revert_pairs)):
                parts = pair.split("~!~")
                u1 = parts[0]
                u2 = parts[1]
                try: 
                    if user_edits[u1]<user_edits[u2]:
                        edit_min = user_edits[u1]
                    else:
                        edit_min = user_edits[u2]
                    score += edit_min
                    if edit_min > max_score:
                        max_score = edit_min
                except:
                    print('blank')
            
            score -= max_score
            i+=1
            if i%1000==0:
                print(i)
            score *= len(mutual_revert_users)
            results[current_page_name[0]]=score
            
            mutual_revert_pairs = []
            
            current_page_name = line
            page_edits = []        
        elif len(line) == 1 and len(page_edits) == 0:
            current_page_name = line
            continue
        else:
            page_edits.append(line)
            
    df1 = pd.DataFrame.from_dict(results,orient='index', columns=['mscore'])
    df1.to_csv('M_stats.csv')