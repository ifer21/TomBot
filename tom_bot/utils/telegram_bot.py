import requests


def telegram_bot_sendtext(bot_message):
    bot_token = ""
    bot_chatID = ""
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def telegram_bot_sendimage(bot_image):
    bot_token = ""
    bot_chatID = ""
    send_ima = 'https://api.telegram.org/bot' + bot_token + '/sendPhoto?Fchat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    'https: // api.telegram.org / bot' + botToken + ' / sendPhoto - Fchat_id = ' + chat_id + " -F photo=@" + imageFile
    response = requests.get(send_ima)

    return response.json()
