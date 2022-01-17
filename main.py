import http.client
import json
import logging
import sqlite3
import os
from telegram.ext import Updater, CommandHandler
from pathlib import Path

Path("config").mkdir(parents=True, exist_ok=True)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

if os.environ.get('TOKEN'):
    Token = os.environ['TOKEN']
    chatid = os.environ['CHATID']
    delay = int(os.environ['DELAY'])
else:
    Token = "X"
    chatid = 0
    delay = 60

if Token == "X":
    print("Token not set!")

messages_dict = {}


def sqlite_connect():
    global conn
    conn = sqlite3.connect('config/messages.db', check_same_thread=False)


def sqlite_load_all():
    sqlite_connect()
    c = conn.cursor()
    c.execute('SELECT * FROM messages')
    rows = c.fetchall()
    conn.close()
    return rows


def sqlite_write(objectid, title, time, location, message):
    sqlite_connect()
    c = conn.cursor()
    q = [(objectid), (title), (time), (location), (message)]
    c.execute('''INSERT INTO messages('objectid', 'title','time','location','message') VALUES(?,?,?,?,?)''', q)
    conn.commit()
    conn.close()


def get_data():
    conn = http.client.HTTPSConnection("services1.arcgis.com")
    payload = ''
    headers = {}
    conn.request("GET", "/rhs5fjYxdOG1Et61/ArcGIS/rest/services/Hairio/FeatureServer/0/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=102100&resultOffset=0&resultRecordCount=50&cacheHint=true&quantizationParameters=%7B%22mode%22:%22edit%22%7D", payload, headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")
    jsondata = json.loads(data)
    return jsondata["features"]



def messages_load():
    # if the dict is not empty, empty it.
    if bool(messages_dict):
        messages_dict.clear()

    for row in sqlite_load_all():
        messages_dict[row[0]] = (row[1], row[2], row[3], row[4])


def messages_monitor(context):
    messages_d = get_data()
    if messages_d:
        for message in messages_d:
            if message['attributes']['OBJECTID'] not in messages_dict:
                sqlite_write(message['attributes']['OBJECTID'], message['attributes']['TIT'], message['attributes']['TIM'], message['attributes']['LDT'], message['attributes']['MES'])
                messages_dict[message['attributes']['OBJECTID']] = (message['attributes']['TIT'], message['attributes']['TIM'], message['attributes']['LDT'], message['attributes']['MES'])
                context.bot.send_message(chatid,
                    str(message['attributes']['OBJECTID']) + " - " + message['attributes']['TIT'] + "\n" +
                    message['attributes']['TIM'] + "\n" +
                    message['attributes']['LDT'] + "\n" +
                    message['attributes']['MES']
                )
    return


def cmd_test(update, context):
    conn = http.client.HTTPSConnection("services1.arcgis.com")
    payload = ''
    headers = {}
    conn.request("GET", "/rhs5fjYxdOG1Et61/ArcGIS/rest/services/Hairio/FeatureServer/0/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=102100&resultOffset=0&resultRecordCount=50&cacheHint=true&quantizationParameters=%7B%22mode%22:%22edit%22%7D", payload, headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")
    jsondata = json.loads(data)
    context.bot.send_message(chatid,
        str(jsondata["features"][-1]['attributes']['OBJECTID']) + " - " + jsondata["features"][-1]['attributes']['TIT'] + "\n" +
        jsondata["features"][-1]['attributes']['TIM'] + "\n" +
        jsondata["features"][-1]['attributes']['LDT'] + "\n" +
        jsondata["features"][-1]['attributes']['MES']
    )
    conn.close()
    return

def init_sqlite():
    conn = sqlite3.connect('config/messages.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE messages (objectid integer unique, title text, time text, location text, message text)''')


def main():
    updater = Updater(token=Token, use_context=True)
    job_queue = updater.job_queue
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("test", cmd_test, ))

    try:
        init_sqlite()
    except sqlite3.OperationalError:
        pass
    messages_load()

    job_queue.run_repeating(messages_monitor, delay)

    updater.start_polling()
    updater.idle()


if __name__ == '__main__':

    main()
