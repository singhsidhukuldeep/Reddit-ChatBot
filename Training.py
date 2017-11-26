'''
CREDITS
Kuldeep Singh Sidhu
Github: github/singhsidhukuldeep https://github.com/singhsidhukuldeep
Website: Kuldeep Singh Sidhu (Website) http://kuldeepsinghsidhu.esy.es
LinkedIn: Kuldeep Singh Sidhu (LinkedIn) https://www.linkedin.com/in/kuldeep-singh-sidhu-96a67170/
Repository: github.com/singhsidhukuldeep/Reddit-ChatBot
Language Used: Python 3.6.1
Compiler Used:
'''
credits = 'CREDITS\nKuldeep Singh Sidhu\n \nGithub:\t github/singhsidhukuldeep https://github.com/singhsidhukuldeep\nWebsite:\t Kuldeep Singh Sidhu (Website) http://kuldeepsinghsidhu.esy.es\nLinkedIn:\t Kuldeep Singh Sidhu (LinkedIn) https://www.linkedin.com/in/kuldeep-singh-sidhu-96a67170/\nRepository:\t github.com/singhsidhukuldeep/Reddit-ChatBot\nLanguage Used:\t Python 3.6.1\nCompiler Used:\t\n\n'

print (credits)

#------------------------------------------------------

import sqlite3
import pandas as pd

timeframes = ['2015-05']

for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while cur_length == limit:

        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)

        if not test_done:
            with open('test.from','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')

            with open('test.to','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content)+'\n')

            test_done = True

        else:
            with open('train.from','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')

            with open('train.to','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content)+'\n')

        counter += 1
        if counter % 20 == 0:
            print(counter*limit,'rows completed so far')

#----------------------------------------------
print (credits)
