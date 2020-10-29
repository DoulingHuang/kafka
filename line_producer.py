from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)
from confluent_kafka import Producer
import sys
app = Flask(__name__)

line_bot_api = LineBotApi('FNquP8lMYOLYYIXwabGzmv7guo6hTTmoxuLs54M909JN4Cr5cg5lPNUefoSWhDsNekLZ8PqJDb8sDwwqvJYtD2oUUAC4eqMSBVoEO6dI2136vEGKiLSwZYbK1hLKbTVcct8EJc5fzJoyitvXy2vv5AdB04t89/1O/w1cDnyilFU=')
handler = WebhookHandler('b7c213400a79e9894d891b77e2eab00a')


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)

    return 'OK'


def error_cb(err):
    print('Error: %s' % err)
import re
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text))
    # if (event.message.text.find("{'user_name': 's8clark', 'score': '3.5', 'text': 'null'}") != -1):
    #     line_bot_api.reply_message(
    #         event.reply_token,
    #         TextSendMessage(text='OK')
    #     )
    # else:
    #     line_bot_api.reply_message(
    #         event.reply_token,
    #         TextSendMessage(text="請點擊菜單上圖面，或輸入[::text:]more，取得更多幫助")
    #     )
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'testdb'
    msgCounter = 0
    try:
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        # '{"user_name" : "Douling","score" : "3.5","text" : "this is good"}'
        producer.produce(topicName, event.message.text)
        producer.flush()
        msgCounter += 4
        print('Send ' + str(msgCounter) + ' messages to Kafka')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()

if __name__ == '__main__':
    app.run()