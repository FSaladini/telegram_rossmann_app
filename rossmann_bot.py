import os
import pandas as pd
import json
import requests
from flask import Flask, request, Response

# creating requests
TOKEN = '6365126057:AAFKsJLypJmQXNzTNY5TH9pFARiyNq1toOs'

# info about the bot
# https://api.telegram.org/bot6365126057:AAFKsJLypJmQXNzTNY5TH9pFARiyNq1toOs/getMe

# get updates
# https://api.telegram.org/bot6365126057:AAFKsJLypJmQXNzTNY5TH9pFARiyNq1toOs/getUpdates

# WebHook
# https://api.telegram.org/bot6365126057:AAFKsJLypJmQXNzTNY5TH9pFARiyNq1toOs/setWebhook?url=https://8f1de52cbca018.lhr.life

# send message
# https://api.telegram.org/bot6365126057:AAFKsJLypJmQXNzTNY5TH9pFARiyNq1toOs/sendMessage?chat_id=2024036880&text=Opa, to bem


def send_message( chat_id, text ):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format( chat_id )

    r = requests.post( url, json={'text': text} )
    print('Status Code {}'.format( r.status_code ) )

    return None

def load_dataset( store_id ):
    # loading test dataset
    df11 = pd.read_csv('data/test.csv', low_memory=False)
    df_store_raw = pd.read_csv('data/store.csv', low_memory=False)

    # merging data
    df_test = pd.merge( df11, df_store_raw, how='left', on='Store' )

    # chose store for test prediction
    df_test = df_test[df_test['Store'] == store_id ]

    if not df_test.empty:

        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop( 'Id', axis=1 )

        data = json.dumps( df_test.to_dict( orient='records' ) )
    else:
        data = 'Error'

    return data

def predict( data ):
    # API Call
    url = 'https://rossmann-api-cuko.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json' }
    data = data

    r = requests.post( url, data, headers = header )
    print( 'Status Code {}'.format( r.status_code ) )

    d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )

    return d1


def parse_message( message ):
    chat_id = message['message']['from']['id']

    store_id = message['message']['text']
    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    except ValueError:
        store_id = 'Error'
    
    return chat_id, store_id


# API Initialize
app = Flask(  __name__ )

@app.route( '/', methods=['GET', 'POST'] )
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message( message )

        if store_id != 'Error':
            # load data
            data = load_dataset( store_id )

            if data != 'Error':
                # predict
                d1 = predict( data )
                # calculate
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                
                msg = ('Store number {} will sell R${:,.2f} in the next 6 weeks.'.format( d2['store'].values[0], d2['prediction'].values[0] ) )
                send_message( chat_id, msg )
                return Response( 'Ok', status=200)
                
            else:
                send_message( chat_id, 'Store not available' )
                return Response( 'Ok', status=200)

        else:
            send_message( chat_id, 'Store ID does not compute')
            return Response( 'Ok', status=200)

    else:
        return '<h1> Rossmann Telegram Bot! </h1>'

if __name__ == '__main__':
    port = os.environ.get( 'PORT', 10000 )
    app.run( host='0.0.0.0', port=port)
