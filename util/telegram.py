import requests

# bot_chatID = '-578237898'  # TEST GROUP
bot_chatID = '-423666828'  # ATTIMO 3.0 CHAT
TELEGRAM_TOKEN = '1255391215:AAGBWEosmnyPeh-iO-_sydOjZWYEZC_q2mo'
API_URL = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}' + '/sendDocument'


def send_message(bot_message):
    send_text = 'https://api.telegram.org/bot' + TELEGRAM_TOKEN + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)
    # print(bot_message)
    return response.json()